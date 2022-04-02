package lnsim

import (
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"unsafe"

	"git.tu-berlin.de/dergoegge/bachelor_thesis/netsim"
)

const (
	InvSize    int = 8
	UpdateSize int = 128
)

type SeenMetaData struct {
	Count     int
	Broadcast bool
}

type LightningNode struct {
	netNode *netsim.Node

	// BQ or BroadcastQueue holds the messages that are supposed to be Broadcast.
	// message id -> messages with senders for deduplication
	//
	// For staggered broadcast all messages in this queue will be broadcast
	// almost at the same time. This clears the map entirely.
	//
	// For set reconciliation we keep the same map around and only delete messages
	// if they were seen by all our gossip peers.
	BQ map[uint64]MessageWithSenders

	// Seen is used to track metadata for messages a node has seen.
	// message id -> metadata (seen counter, Broadcast)
	Seen map[uint64]SeenMetaData

	// SeenByChannel tracks the latest channel_update message seen by channel.
	// channel id -> latest messages seen for that channel (2 messages, one for each direction)
	SeenByChannel map[int][2]*LightningMessage

	// Stagger: Peers that have asked this node to send new gossip messages.
	// Reconciliation: Peers that this node sends sketches to.
	GossipPeers map[int]netsim.Noder

	// Reconciliation ticker data with peers
	// len(GossipTickers) == 0 if we are not simulating reconciliation
	GossipTickers map[int]struct {
		peerID     int
		lastTicked uint64
		willTick   bool
	}

	// Reconciliation sets
	// peer id -> set
	// One set for every peer we reconciliate with
	ReconcilSets map[int]*ReconcilSet

	// Messages that were requested via a query.
	// Used to make sure we only download a message once when working with invs.
	RequestedInv map[int]struct{}

	S          *netsim.Simulation
	LNS        *LightingNetworkSimulation
	willTick   bool
	tickOffset uint64
}

func NewLightningNode(netNode *netsim.Node,
	lnSim *LightingNetworkSimulation,
	netSim *netsim.Simulation) *LightningNode {
	return &LightningNode{
		netNode:      netNode,
		S:            netSim,
		LNS:          lnSim,
		GossipPeers:  make(map[int]netsim.Noder),
		ReconcilSets: make(map[int]*ReconcilSet),
		RequestedInv: make(map[int]struct{}),
		GossipTickers: make(map[int]struct {
			peerID     int
			lastTicked uint64
			willTick   bool
		}),
		BQ:            make(map[uint64]MessageWithSenders),
		Seen:          make(map[uint64]SeenMetaData),
		SeenByChannel: make(map[int][2]*LightningMessage),
		willTick:      true,
		tickOffset:    uint64(rand.Intn(int(GlobalConfig.Algorithm.StaggerTimer) - 1)),
	}
}

func (ln *LightningNode) InitStaggerTicker() {
	ln.S.NewNodeTicker(GlobalConfig.Algorithm.StaggerTimer, 0, ln.netNode.ID, ln.tickOffset)
}

func (ln *LightningNode) InitReconcilTicker(p *LightningNode) {
	if GlobalConfig.Algorithm.ReconcilTimer == 0 {
		return
	}

	ticker := struct {
		peerID     int
		lastTicked uint64
		willTick   bool
	}{
		lastTicked: 0,
		willTick:   true,
		peerID:     p.NetNode().ID,
	}
	ln.GossipTickers[p.NetNode().ID] = ticker

	var peerID interface{}
	peerID = p.NetNode().ID
	ln.S.NewNodeTickerWithData(
		uint64((len(ln.GossipTickers)-1)*int(GlobalConfig.Algorithm.ReconcilTimer)+1),
		0xdeadbeef,
		ln.netNode.ID,
		peerID,
	)
}

func (ln *LightningNode) RequestGossipFromNode(other *LightningNode) {
	other.GossipPeers[ln.netNode.ID] = ln
	if GlobalConfig.Algorithm.ReconcilTimer > 0 {
		fmt.Println(other.netNode.ID, "-->", ln.netNode.ID)
		// Init our reconcil set and theirs
		ln.ReconcilSets[other.NetNode().ID] = NewReconcilSet(other.NetNode().ID)
		other.ReconcilSets[ln.netNode.ID] = NewReconcilSet(ln.netNode.ID)
		// Schedule the reconcil request ticker for us
		ln.InitReconcilTicker(other)
	}

}

func GetNextTick(currTime, lastTick, tickInterval uint64) uint64 {
	nextTick := lastTick + tickInterval

	for nextTick < currTime {
		nextTick += tickInterval
	}

	if nextTick-currTime > tickInterval {
		panic(fmt.Errorf(
			"interval can't be more than broadcast interval %d (%d, %d, %d)",
			nextTick-currTime, nextTick, currTime, lastTick,
		))
	}

	return nextTick
}

func (ln *LightningNode) ScheduleStaggerTick(currTime uint64) {
	if GlobalConfig.Algorithm.ReconcilTimer == 0 && !ln.willTick {
		// Register tick
		/*
			=:==.=|=:===|=:===|=:===|=:===|=:...
		*/

		firstTick := GlobalConfig.Algorithm.StaggerTimer - ln.tickOffset
		timeSinceFirstTick := currTime - firstTick
		timesTicked := uint64(timeSinceFirstTick/GlobalConfig.Algorithm.StaggerTimer) + 1
		lastTick := timesTicked*GlobalConfig.Algorithm.StaggerTimer - ln.tickOffset
		nextTick := GetNextTick(currTime, lastTick, GlobalConfig.Algorithm.StaggerTimer)

		ln.LNS.NS.NewNodeTicker(
			nextTick-currTime,
			0,
			ln.netNode.ID,
			0,
		)
		ln.willTick = true
	}
}

func (ln *LightningNode) IsBlackhole() bool {
	return len(ln.GossipPeers) == 0
}

func (ln *LightningNode) MakeBlackhole() {
	ln.GossipPeers = make(map[int]netsim.Noder)
}

func (ln *LightningNode) NetNode() *netsim.Node {
	return ln.netNode
}

func calculateSubBatchSize(totalDelay, subBatchDelay uint64,
	minimumBatchSize, batchSize int) int {
	if subBatchDelay > totalDelay {
		return batchSize
	}

	subBatchSize := (int(batchSize)*int(subBatchDelay) + int(totalDelay) - 1) /
		int(totalDelay)

	if subBatchSize < minimumBatchSize {
		return minimumBatchSize
	}

	return subBatchSize
}

func (ln *LightningNode) SetActiveSyncerForST(other *LightningNode) {
	other.RequestGossipFromNode(ln)
	ln.RequestGossipFromNode(other)
}

func (ln *LightningNode) ChooseActiveSyncers() {
	peers := ln.netNode.Peers
	if GlobalConfig.Algorithm.ReconcilTimer > 0 {
		// Pick syncers from outbound peers only with set recon.
		peers = ln.netNode.OutboundPeers
	}

	peerKeys := make([]int, 0)
	for k := range peers {
		peerKeys = append(peerKeys, k)
	}
	sort.Ints(peerKeys)

	if len(peerKeys) < int(GlobalConfig.SyncerConnections) {
		panic("less than n needed peers")
	}

	for i := 0; i < int(GlobalConfig.SyncerConnections); i++ {
		randInt := rand.Intn(len(peerKeys))
		for j, peerKey := range peerKeys {
			if j != randInt {
				continue
			}

			peer := ln.netNode.Peers[peerKey].(*LightningNode)
			ln.RequestGossipFromNode(peer)

			peerKeys = append(peerKeys[:j], peerKeys[j+1:]...)
			break
		}
	}
}

func (ln *LightningNode) StaggerTick(time uint64, t netsim.Ticker) bool {
	if GlobalConfig.Algorithm.ReconcilTimer > 0 {
		panic("should not stagger while reconciling")
	}

	batchSize := calculateSubBatchSize(
		t.Interval,
		GlobalConfig.Algorithm.TrickleTimer,
		GlobalConfig.Algorithm.MinBatchSize,
		len(ln.BQ),
	)

	var recordWaitingTimes bool = true

	gossipPeerKeys := make([]int, 0, len(ln.GossipPeers))
	bqKeys := make([]uint64, 0, len(ln.BQ))
	if len(ln.BQ) == 0 {
		goto end
	}

	for k := range ln.GossipPeers {
		gossipPeerKeys = append(gossipPeerKeys, k)
	}
	for k := range ln.BQ {
		bqKeys = append(bqKeys, k)
	}
	sort.Ints(gossipPeerKeys)
	sort.Slice(bqKeys, func(i, j int) bool { return bqKeys[i] < bqKeys[j] })

	for _, k := range gossipPeerKeys {
		p, ok := ln.GossipPeers[k]
		if !ok {
			panic("Tick: gossip peer does not exist")
		}

		currBatch := 0
		timeOffset := uint64(0)
		if p.NetNode().ID == ln.netNode.ID {
			panic(fmt.Errorf("Tick: cant send message to your self %d", ln.netNode.ID))
		}

		var batch []*LightningMessage = nil
		if GlobalConfig.Algorithm.SendBatchUpdates {
			batch = []*LightningMessage{}
		}
		var invs []ChannelInv = nil
		if GlobalConfig.Algorithm.SendInvs {
			invs = []ChannelInv{}
		}

		msgIndex := 0
		for _, k := range bqKeys {
			m := ln.BQ[k]
			if ln.Seen[m.m.ID()].Broadcast {
				panic("messages should never be broadcast twice")
			}
			if time-m.arrivalTime > t.Interval {
				panic("message can not have been in the queue longer than the ticker interval")
			}

			_, ok := m.senders[p.NetNode().ID]

			if recordWaitingTimes {
				WaitedLogger.Log([]string{
					strconv.FormatUint(time, 10),
					strconv.Itoa(ln.netNode.ID),
					strconv.Itoa(int(m.m.ID())),
					strconv.FormatUint((time+timeOffset)-m.arrivalTime, 10),
					strconv.FormatBool(ok),
				})

				WaitedBuckets.Put((time + timeOffset) - m.arrivalTime)
			}

			// lnd checks the senders right before the batch is send,
			// so a batch also contains msgs that wont be send.
			if GlobalConfig.Algorithm.ShouldTrickle && currBatch == batchSize {
				if GlobalConfig.Algorithm.SendBatchUpdates && len(batch) > 0 {
					m := &LightningMessage{
						Type:     ChannelUpdateBatch,
						Batch:    batch,
						DataSize: uint64(UpdateSize * len(batch)),
					}

					ln.S.N.SendMessage(
						time, time+timeOffset, &ln.S.EQ,
						ln.netNode.ID, p.NetNode().ID, m,
					)
					batch = []*LightningMessage{}
				} else if GlobalConfig.Algorithm.SendInvs && len(invs) > 0 {
					m := &LightningMessage{
						Type:     ChannelInvs,
						Invs:     invs,
						DataSize: uint64(InvSize * len(invs)),
					}
					ln.S.N.SendMessage(
						time, time+timeOffset, &ln.S.EQ,
						ln.netNode.ID, p.NetNode().ID, m,
					)
					invs = []ChannelInv{}
				}

				timeOffset += GlobalConfig.Algorithm.TrickleTimer
				currBatch = 0
			}

			currBatch++
			msgIndex++

			if ok {
				// Dont send messages back to originators
				continue
			}

			if GlobalConfig.Algorithm.SendBatchUpdates {
				batch = append(batch, m.m.(*LightningMessage))
			} else if GlobalConfig.Algorithm.SendInvs {
				lm := m.m.(*LightningMessage)
				invs = append(invs, ChannelInv{
					ChannelID:     lm.ChannelID,
					Timestamp:     lm.Policy.LastUpdate,
					Direction:     lm.Direction,
					IsNotAnUpdate: lm.Type != ChannelUpdate,
					MsgID:         lm.Id,
				})
			} else {
				ln.S.N.SendMessageWithHops(
					time, time+timeOffset, &ln.S.EQ,
					ln.netNode.ID, p.NetNode().ID, m.m, m.hops,
				)
			}
		}
		if GlobalConfig.Algorithm.SendBatchUpdates && len(batch) > 0 {
			m := &LightningMessage{
				Type:     ChannelUpdateBatch,
				Batch:    batch,
				DataSize: uint64(UpdateSize * len(batch)),
			}

			ln.S.N.SendMessage(
				time, time+timeOffset, &ln.S.EQ,
				ln.netNode.ID, p.NetNode().ID, m,
			)
			batch = []*LightningMessage{}
		} else if GlobalConfig.Algorithm.SendInvs && len(invs) > 0 {
			m := &LightningMessage{
				Type:     ChannelInvs,
				Invs:     invs,
				DataSize: uint64((8 + 8 + 1) * len(invs)),
			}
			ln.S.N.SendMessage(
				time, time+timeOffset, &ln.S.EQ,
				ln.netNode.ID, p.NetNode().ID, m,
			)
			invs = []ChannelInv{}
		}
		// Only record waiting times for one peer to avoid redundant data.
		recordWaitingTimes = false
	}

	// Mark all messages as Broadcast.
	// Allows us to verify that we never broadcast the same message twice.
	for _, m := range ln.BQ {
		md, _ := ln.Seen[m.m.ID()]
		md.Broadcast = true
		ln.Seen[m.m.ID()] = md
	}

	ln.BQ = make(map[uint64]MessageWithSenders)

end:
	ln.willTick = false
	return false
}

func (ln *LightningNode) Tick(time uint64, t netsim.Ticker) bool {
	if t.Type == 0xdeadbeef {
		return ln.ReconcileTick(time, t)
	}

	return ln.StaggerTick(time, t)
}

func (ln *LightningNode) IsNewMessage(m *LightningMessage) bool {
	md, seen := ln.Seen[m.ID()]
	if !seen {
		return true
	}

	return md.Count == 0
}

func (ln *LightningNode) HandleUpdate(time uint64, m netsim.Message, sender netsim.Noder, hops uint64) {
	if int(m.TimesSeen()) >= len(ln.netNode.N.Nodes) {
		// This message was already seen by everyone, including this node.
		return
	}

	if !m.(*LightningMessage).Type.IsGossip() {
		panic("HandleUpdate: can only handle gossip messages")
	}

	// Delete message from requested invs

	isNew := ln.IsNewMessage(m.(*LightningMessage))

	md, _ := ln.Seen[m.ID()]
	md.Count++
	ln.Seen[m.ID()] = md

	if !isNew && GlobalConfig.Algorithm.ReconcilTimer == 0 {
		ms, ok := ln.BQ[m.ID()]
		if ok {
			ms.senders[sender.NetNode().ID] = sender
			if ms.hops > hops {
				ms.hops = hops
			}
			ln.BQ[m.ID()] = ms
		}
		return
	} else if !isNew {
		if m.(*LightningMessage).Type == ChannelUpdate &&
			GlobalConfig.Algorithm.ReconcilTimer > 0 {
			// For reconcil this should almost never happen.
			fmt.Println("we should not receive updates more than once")
		}
	}

	if isNew {
		// This is the first time we see this message.

		SeenLogger.Log([]string{
			strconv.FormatUint(time-m.(*LightningMessage).BroadcastTime, 10),
			strconv.Itoa(ln.netNode.ID),
			strconv.FormatUint(m.ID(), 10),
			strconv.FormatUint(hops, 10),
		})

		// Record conv times in buckets
		ConvBuckets.Put(time - m.(*LightningMessage).BroadcastTime)

		// This is the very first time we see this message.
		m.(*LightningMessage).Seen++

		if int(m.TimesSeen()) >= len(ln.netNode.N.Nodes) {
			// All nodes have seen this message.
			// => delete it from the network
			ln.netNode.N.DeleteMessage(m)

			// In theory this node would broadcast the message one last time,
			// but we skip it, as thats easier and it won't make a dent in the data.
			return
		}

		// Store the message in SeenByChannel
		var lnm *LightningMessage = m.(*LightningMessage)

		var replace *LightningMessage = nil
		var ok bool = true
		if lnm.Type == ChannelUpdate {
			replace, ok = ln.NewUpdate(lnm)
		}

		if ok {
			senders := make(map[int]netsim.Noder)
			senders[sender.NetNode().ID] = sender
			if GlobalConfig.Algorithm.ReconcilTimer > 0 {
				// Store the message in all reconcil sets beside the one of the sender.
				ln.AddMessageToReconcilSets(replace, lnm, sender)
				ln.ScheduleReconcilTicks(time)
			} else {
				// for reconciliation we dont use the BQ
				ln.BQ[m.ID()] = MessageWithSenders{m, senders, hops, time}
				ln.ScheduleStaggerTick(time)
			}
		}
	}
}

func (ln *LightningNode) ShouldSendQuery(inv ChannelInv) bool {
	_, requested := ln.RequestedInv[int(inv.MsgID)]
	md, seen := ln.Seen[inv.MsgID]

	return !(requested || (seen && md.Count > 0))
}

func (ln *LightningNode) SendQuery(invs []ChannelInv, time uint64, to netsim.Noder) {
	// Check if we have the channels and send a query if needed.
	sendInvs := make([]ChannelInv, 0, len(invs))
	for _, inv := range invs {
		if !ln.ShouldSendQuery(inv) {
			// We sent a query for this message and are still waiting for a response.
			// Do not request invs twice
			continue
		}

		if inv.IsNotAnUpdate {
			sendInvs = append(sendInvs, inv)
			continue
		}

		msgs, ok := ln.SeenByChannel[inv.ChannelID]
		if !ok {
			sendInvs = append(sendInvs, inv)
			continue
		}

		m := msgs[inv.Direction]
		if m == nil {
			sendInvs = append(sendInvs, inv)
			continue
		}

		if m.Policy.LastUpdate < inv.Timestamp {
			sendInvs = append(sendInvs, inv)
			continue
		}
	}

	if len(sendInvs) > 0 {
		for _, inv := range sendInvs {
			ln.RequestedInv[int(inv.MsgID)] = struct{}{}
		}

		res := &LightningMessage{
			Type:          ChannelQuery,
			Invs:          sendInvs,
			DataSize:      uint64(InvSize * len(sendInvs)),
			BroadcastTime: time,
		}

		ln.S.N.SendMessage(
			time, time, &ln.S.EQ,
			ln.netNode.ID,
			to.NetNode().ID, res)
	}
}

func (ln *LightningNode) HandleQuery() {}
func (ln *LightningNode) Receive(time uint64, m netsim.Message, sender netsim.Noder, hops uint64) {
	msgType := m.(*LightningMessage).Type

	if msgType == ChannelQuery {
		if len(m.(*LightningMessage).Invs) == 0 {
			panic("should not receive empty queries")
		}

		// received channel query
		batch := make([]*LightningMessage, 0, len(m.(*LightningMessage).Invs))
		for _, inv := range m.(*LightningMessage).Invs {
			if inv.IsNotAnUpdate {
				batch = append(batch, ln.LNS.Messages[int(inv.MsgID)])
				continue
			}

			msgs, ok := ln.SeenByChannel[inv.ChannelID]
			if !ok {
				fmt.Println(ln.netNode.ID, ln.ReconcilSets[sender.NetNode().ID], inv)
				panic("1: peer should not request channel that did not have a recent update")
			}
			m := msgs[inv.Direction]
			if m == nil {
				panic("2: peer should not request channel that did not have a recent update")
			}

			md, _ := sender.(*LightningNode).Seen[m.ID()]
			if GlobalConfig.Algorithm.ReconcilTimer > 0 && md.Count > 0 {
				continue
			}

			batch = append(batch, m)
		}

		res := &LightningMessage{
			Type:          ChannelUpdateBatch,
			Batch:         batch,
			DataSize:      uint64(UpdateSize * len(batch)),
			BroadcastTime: time,
		}

		if len(batch) > 0 {
			ln.S.N.SendMessage(
				time, time, &ln.S.EQ,
				ln.netNode.ID,
				sender.NetNode().ID, res)
		}

		return

	} else if msgType == ChannelInvs {
		if m.(*LightningMessage).ReconcilSuccess {
			// Clear reconcil set with this peer
			ln.ReconcilSets[sender.NetNode().ID].Clear()
		}

		ln.SendQuery(m.(*LightningMessage).Invs, time, sender)
		return
	} else if msgType == ChannelUpdateBatch {
		if m.(*LightningMessage).ReconcilSuccess {
			// Clear reconcil set with this peer
			ln.ReconcilSets[sender.NetNode().ID].Clear()
		}

		// Received a batch of channel updates.
		for _, update := range m.(*LightningMessage).Batch {
			// TODO figure out hops for these
			ln.HandleUpdate(time, update, sender, 0)
		}
		return
	} else if msgType == ReconcilRequest {
		ln.HandelReconcilRequest(time, m, sender)
		return
	} else if msgType == ReconcilSketch {
		ln.HandleSketch(time, m, sender)
		return
	}

	ln.HandleUpdate(time, m, sender, hops)
}

func (ln *LightningNode) Cleanup(m netsim.Message) {
	// Delete all traces of this message from the node.
	delete(ln.BQ, m.ID())
	meta := ln.Seen[m.ID()]
	delete(ln.Seen, m.ID())
	// Dump useful data to logs.
	ln.netNode.N.RedundancyLogger.Log([]string{strconv.Itoa(meta.Count)})
}

func (ln *LightningNode) Size() uint64 {
	seenSize := uint64(unsafe.Sizeof(ln.Seen)) + uint64(len(ln.Seen))*uint64(unsafe.Sizeof(SeenMetaData{}))
	byChannelSize := uint64(unsafe.Sizeof(ln.SeenByChannel)) + uint64(len(ln.SeenByChannel))*uint64(unsafe.Sizeof([2]*LightningMessage{}))
	bqSize := uint64(unsafe.Sizeof(ln.BQ)) + uint64(len(ln.BQ))*uint64(unsafe.Sizeof(MessageWithSenders{}))
	return seenSize + byChannelSize + bqSize
}

// Add a new update to SeenByChannel.
// Return the update that we are replacing.
func (ln *LightningNode) NewUpdate(m *LightningMessage) (*LightningMessage, bool) {
	if m.Type != ChannelUpdate {
		panic("NewUpdate: message not an update")
	}

	var replace *LightningMessage = nil

	updates, ok := ln.SeenByChannel[m.ChannelID]
	if ok {
		if updates[m.Direction] == nil || m.Policy.LastUpdate > updates[m.Direction].Policy.LastUpdate {
			replace = updates[m.Direction]
			updates[m.Direction] = m
		} else {
			//fmt.Println("ignoring update", m.ID(), m.Timestamp, updates[m.Direction].Timestamp)
			return replace, false
		}
	} else {
		updates = [2]*LightningMessage{nil, nil}
		updates[m.Direction] = m
	}

	ln.SeenByChannel[m.ChannelID] = updates
	return replace, true
}

// Change a channel policy based on probabilities seen in the real network.
func changePolicy(p *ChannelPolicy) {
	if p.Disabled {
	} else {
	}
	p.Disabled = !p.Disabled
}

// BroadcastChannelUpdate picks a random channel from the node and broadcasts a channel update
// for it at a specific time (when).
func (ln *LightningNode) BroadcastChannelUpdate(m *LightningMessage, when uint64) bool {
	// The graph node of this node.
	graphNode := &ln.LNS.G.Nodes[ln.netNode.ID]

	if len(graphNode.Channels) == 0 {
		// node has no channels
		return false
	}

	// A random channel of the graph node
	randChannel := graphNode.Channels[rand.Intn(len(graphNode.Channels))]
	policy := randChannel.GetPolicy(graphNode)
	changePolicy(&policy)
	policy.LastUpdate = when

	m.ChannelID = randChannel.ChannelID
	m.BroadcastTime = when
	m.Type = ChannelUpdate
	m.Policy = policy
	m.Origin = graphNode
	m.Direction = randChannel.GetDirection(m.Origin)

	// Broadcast message to all peers
	for _, p := range ln.netNode.Peers {
		ln.S.N.SendMessage(ln.S.Time, when, &ln.S.EQ, ln.netNode.ID, p.NetNode().ID, m)
	}

	// Mark as seen by this node
	ln.Seen[m.ID()] = SeenMetaData{Count: 1, Broadcast: true}
	ln.NewUpdate(m)
	return true
}

func (ln *LightningNode) BroadcastMessage(m *LightningMessage) bool {
	if GlobalConfig.NoKeepalives && m.IsKeepalive {
		// Ignore keep alive updates.
		return true
	}

	// Broadcast message to all peers
	if GlobalConfig.Algorithm.SpanningTree {
		for _, p := range ln.GossipPeers {
			ln.S.N.SendMessage(ln.S.Time, m.BroadcastTime, &ln.S.EQ, ln.netNode.ID, p.NetNode().ID, m)
		}
	} else {
		for _, p := range ln.netNode.Peers {
			ln.S.N.SendMessage(ln.S.Time, m.BroadcastTime, &ln.S.EQ, ln.netNode.ID, p.NetNode().ID, m)
		}
	}

	// Mark as seen by this node
	ln.Seen[m.ID()] = SeenMetaData{Count: 1, Broadcast: true}
	if m.Type == ChannelUpdate {
		ln.NewUpdate(m)
	}

	//ConvBuckets.Put(0)
	return true
}

type PaymentStatus int

const (
	// The payment succeeded
	PaymentSuccess PaymentStatus = iota
	// The channel balances along the route were not largee enough
	LiquidityFailure
	// The node did not have upto date routing information in its routing base.
	ConvergenceFailureFeeUp
	ConvergenceFeeDown
	ConvergenceFailureCLTVUp
	ConvergenceCLTVDown
	ConvergenceFailureDisabled
	ConvergenceOKButOld

	// There was no payment route found
	NoRouteFailure
)

func (s PaymentStatus) String() string {
	switch s {
	case PaymentSuccess:
		return "SUCCESS"
	case LiquidityFailure:
		return "LIQUIDITY"
	case ConvergenceFailureFeeUp:
		return "CONV-FEEUP"
	case ConvergenceFeeDown:
		return "CONV-FEEDOWN"
	case ConvergenceFailureCLTVUp:
		return "CONV-CLTVUP"
	case ConvergenceCLTVDown:
		return "CONV-CLTVDOWN"
	case ConvergenceFailureDisabled:
		return "CONV-DISABLED"
	case ConvergenceOKButOld:
		return "CONV-OKOLD"
	case NoRouteFailure:
		return "NOROUTE"
	}

	return "UNKOWN"
}

func (s PaymentStatus) IsSuccess() bool {
	switch s {
	case PaymentSuccess:
		return true
	case ConvergenceFeeDown:
		return true
	case ConvergenceCLTVDown:
		return true
	case ConvergenceOKButOld:
		return true
	default:
		return false
	}
}

func StatusSliceToString(statusPerChannel []PaymentStatus) string {
	str := ""
	for _, status := range statusPerChannel {
		str += status.String() + "|"
	}
	return str
}

func compareChannels(amt uint64, node *GraphNode, actual ChannelPolicy, used ChannelPolicy) PaymentStatus {
	if actual.Disabled {
		return ConvergenceFailureDisabled
	}

	if actual.GetFee(amt) > used.GetFee(amt) {
		return ConvergenceFailureFeeUp
	}
	if actual.GetFee(amt) < used.GetFee(amt) {
		return ConvergenceFeeDown
	}

	if actual.TimeLockDelta > used.TimeLockDelta {
		return ConvergenceFailureCLTVUp
	}
	if actual.TimeLockDelta < used.TimeLockDelta {
		return ConvergenceCLTVDown
	}

	if actual.LastUpdate > used.LastUpdate {
		return ConvergenceOKButOld
	}

	return PaymentSuccess
}

func IsRedundant(p1 ChannelPolicy, p2 ChannelPolicy) bool {
	return p1.Disabled == p2.Disabled &&
		p1.FeeBaseMsat == p2.FeeBaseMsat &&
		p1.MaxHtlcMsat == p2.MaxHtlcMsat &&
		p1.FeeRateMilliMsat == p2.FeeRateMilliMsat &&
		p1.MinHtlc == p1.MinHtlc &&
		p1.TimeLockDelta == p2.TimeLockDelta
}

func getPolicyFromNodeOrGraph(channelID uint64, dir int, graph *ChannelGraph, node *LightningNode) ChannelPolicy {
	updates, ok := node.SeenByChannel[int(channelID)]
	if ok {
		if updates[dir] != nil {
			//fmt.Println("from node", updates[dir].Policy, graph.Channels[channelID].GetPolicyByDir(dir))
			//fmt.Println("From node", updates[dir].Policy, updates[dir].Timestamp)
			return updates[dir].Policy
		}
	}

	//fmt.Println("From graph", graph.Channels[channelID].GetPolicyByDir(dir))
	return graph.Channels[channelID].GetPolicyByDir(dir)
}

func (ln *LightningNode) CheckPayment(p PaymentPath) (statusPerChannel []PaymentStatus) {
	if len(p.Hops) == 0 {
		return []PaymentStatus{NoRouteFailure}
	}

	current := p.Src

	statusPerChannel = []PaymentStatus{}

	for i, c := range p.Hops {
		amt := p.Amounts[i]
		balance := uint64(0)
		next := current
		if current == c.Node1 {
			balance = c.Node1Balance
			next = c.Node2
		} else if current == c.Node2 {
			balance = c.Node2Balance
			next = c.Node1
		} else {
			panic("hops not connected")
		}

		if current == next {
			panic("current == next")
		}

		/*fmt.Println("==============HOP================")
		fmt.Println("channel", c.ChannelID)
		fmt.Println("current", current.NetworkID)
		fmt.Println("next", next.NetworkID)
		fmt.Println("dir", c.GetDirection(current))
		fmt.Println("src", p.Src.NetworkID)
		fmt.Println("policy", c.GetPolicy(current))
		fmt.Println(c)*/

		if c.GetPolicy(current).Disabled {
			panic("should not choose disabled edges for path")
		}

		if amt > balance*1000 {
			statusPerChannel = append(statusPerChannel, LiquidityFailure)
			return
		}

		// Compate the actual policy to one used for routing the payment.
		currentNetNode := ln.LNS.NS.N.Nodes[current.NetworkID].(*LightningNode)
		if i == 0 && currentNetNode != ln {
			panic("1st node has to be the src")
		}

		//otherNetNode := ln.LNS.NS.N.Nodes[next.NetworkID].(*LightningNode)
		actualPolicy := getPolicyFromNodeOrGraph(uint64(c.ChannelID), c.GetDirection(current), &ln.LNS.G, currentNetNode)

		if i == 0 && !IsRedundant(actualPolicy, c.GetPolicy(current)) {
			fmt.Printf("nodeID: %d\nchannelID: %d\nactual policy: %v\npolicy used: %v\nother edge: %v\n",
				ln.netNode.ID, c.ChannelID, actualPolicy, c.GetPolicy(current), c.GetPolicy(next))
			panic("policy has to be the same on the first node")
		}

		status := compareChannels(p.AggAmt, current, actualPolicy, c.GetPolicy(current))
		statusPerChannel = append(statusPerChannel, status)
		if !status.IsSuccess() {
			return
		}

		current = next
	}

	if current != p.Dst {
		panic("did not arrive at dst")
	}

	return
}

var TotalPayments int
var SuccesfulPayments int
var LiquidityFailures int
var NoRouteFailures int
var ConvergenceFailures int

func (ln *LightningNode) SendPayment(amt uint64, dst *LightningNode, time uint64) {
	path := FindPaymentPath(ln.netNode.ID, dst.netNode.ID, amt, 1000, ln.LNS)
	status := ln.CheckPayment(path)
	PaymentLogger.MustLog([]string{
		strconv.FormatUint(time, 10),
		strconv.Itoa(ln.netNode.ID),
		strconv.Itoa(dst.netNode.ID),
		strconv.FormatUint(amt, 10),
		StatusSliceToString(status),
	})

	if TotalPayments > 0 && TotalPayments%200 == 0 {
		log.Printf("[%d] %d total payments (%.3f%% success, %.3f%% liquidity, %.3f%% noroute, %f%% convergence)\n", time, TotalPayments,
			float64(SuccesfulPayments)*100.0/float64(TotalPayments),
			float64(LiquidityFailures)*100.0/float64(TotalPayments),
			float64(NoRouteFailures)*100.0/float64(TotalPayments),
			float64(ConvergenceFailures)*100.0/float64(TotalPayments))
	}

	switch status[len(status)-1] {
	case PaymentSuccess:
		SuccesfulPayments++
		break
	case ConvergenceFailureFeeUp:
		ConvergenceFailures++
		break
	case ConvergenceFeeDown:
		ConvergenceFailures++
		break
	case ConvergenceFailureCLTVUp:
		ConvergenceFailures++
		break
	case ConvergenceCLTVDown:
		ConvergenceFailures++
		break
	case ConvergenceOKButOld:
		ConvergenceFailures++
		break
	case ConvergenceFailureDisabled:
		ConvergenceFailures++
		break
	case LiquidityFailure:
		LiquidityFailures++
		break
	case NoRouteFailure:
		NoRouteFailures++
		break
	default:
	}
	TotalPayments++
}

type LightningMessageType int

const (
	ChannelAnnouncement LightningMessageType = iota
	NodeAnnouncement
	ChannelUpdate
	ChannelQuery

	// These don't actually exist and are for experiments.

	// Package multiple channel_updates into one message.
	ChannelUpdateBatch

	// Send a batch of channel inventory messages.
	// list of short channel ids, their timestamps and direction
	ChannelInvs

	ReconcilSketch
	ReconcilRequest
)

func (t LightningMessageType) String() string {
	switch t {
	case ChannelAnnouncement:
		return "channel_announcement"
	case NodeAnnouncement:
		return "node_announcement"
	case ChannelUpdate:
		return "channel_update"
	case ChannelQuery:
		return "channel_query"
	case ChannelInvs:
		return "channel_invs"
	case ChannelUpdateBatch:
		return "channel_update_batch"
	case ReconcilRequest:
		return "reconcil_req"
	case ReconcilSketch:
		return "reconcil_sketch"
	}

	return "unkown"
}

func (t LightningMessageType) IsGossip() bool {
	switch t {
	case NodeAnnouncement:
		return true
	case ChannelUpdate:
		return true
	case ChannelAnnouncement:
		return true
	}
	return false
}

type ChannelInv struct {
	ChannelID int
	Direction int
	Timestamp uint64

	// This is cheating but to support channel and node annoucements
	// we support "ChannelInv"s for them as well and look up the messages
	// from the LightningNetworkSimulation.Messages field.
	IsNotAnUpdate bool
	MsgID         uint64
}

type LightningMessage struct {
	Id uint64

	Seen uint64

	DataSize uint64

	BroadcastTime uint64

	// When replying messages we sometimes can't find a channel from one of the
	// messages in our snapshot, so we asign it a random channel as origin.
	RandomSCID bool

	Type LightningMessageType

	// For channel updates

	Direction int
	// ChannelID is the index of the channel in the channel graph
	ChannelID int
	// The origin of the message.
	// Used to determine which edge should be updated
	Origin *GraphNode
	// The new channel policy (fees, disabled, ...)
	Policy ChannelPolicy

	// For channel invs
	Invs []ChannelInv

	// For channel update batch
	Batch []*LightningMessage

	IsReconcilResult bool
	ReconcilSuccess  bool

	// Used for diff size estimation.
	// sketch capacity should be:
	// c = |set_size - local_set_size| + Q * (set_size + local_set_size) + 1
	ReconcilSetSize int
	ReconcilQ       float64

	IsKeepalive bool
}

type MessageWithSenders struct {
	// The message that was received.
	m netsim.Message
	// The nodes from which the message has been received.
	//
	// alternativee description:
	// nodes that are known to have the message, either because
	// we sent it to them or they sent it to us.
	senders map[int]netsim.Noder
	// How many hops has this message traveled already?
	hops uint64
	// When did this message first arrive.
	arrivalTime uint64
}

func (msg *LightningMessage) Data() interface{} { return nil }

func (msg *LightningMessage) Size() uint64 { return msg.DataSize }

func (msg *LightningMessage) ID() uint64 { return msg.Id }

func (msg *LightningMessage) TimesSeen() uint64 { return msg.Seen }

type PaymentEvent struct {
	Src, Dst *LightningNode
	Amount   uint64
}

func (pe PaymentEvent) Run(eq *netsim.EventQueue, time uint64) {
	pe.Src.SendPayment(pe.Amount, pe.Dst, time)
}
