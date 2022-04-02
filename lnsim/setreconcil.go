package lnsim

import (
	"fmt"
	"math"

	"git.tu-berlin.de/dergoegge/bachelor_thesis/netsim"
)

type ReconcilSet struct {
	MsgIDs map[int]struct{}
	PeerID int
}

func NewReconcilSet(peerID int) *ReconcilSet {
	return &ReconcilSet{
		PeerID: peerID,
		MsgIDs: make(map[int]struct{}),
	}
}

func (rs *ReconcilSet) Size() int {
	return len(rs.MsgIDs)
}

func (rs *ReconcilSet) Clear() int {
	l := len(rs.MsgIDs)
	rs.MsgIDs = make(map[int]struct{})
	return l
}

func (rs *ReconcilSet) Remove(m *LightningMessage) bool {
	delete(rs.MsgIDs, int(m.ID()))
	return true
}

func (rs *ReconcilSet) Add(m *LightningMessage) bool {
	rs.MsgIDs[int(m.ID())] = struct{}{}
	return true
}

// We do not actually use minisketch and figure out the set diffs
// by going comparing the actual sets.
// TODO: fail this function if the estimated diff is smaller than
// the actual diff.
func (rs *ReconcilSet) Diff(other *ReconcilSet) ([]int, []int) {
	// message IDs that only other has
	need := make([]int, 0, 100)
	// message IDs that only rs has
	have := make([]int, 0, 100)

	for otherID := range other.MsgIDs {
		_, ok := rs.MsgIDs[otherID]
		if !ok {
			need = append(need, otherID)
		}
	}

	for rsID := range rs.MsgIDs {
		_, ok := other.MsgIDs[rsID]
		if !ok {
			have = append(have, rsID)
		}
	}

	return need, have
}

var ReconcileSuccess int = 0
var ReconcileFail int = 0

func (ln *LightningNode) HandleSketch(time uint64, m netsim.Message, sender netsim.Noder) {
	lnSet, ok := ln.ReconcilSets[sender.NetNode().ID]
	if !ok {
		panic("received sketch from peer we dont have a reconcil set for")
	}
	senderSet, ok := sender.(*LightningNode).ReconcilSets[ln.NetNode().ID]
	if !ok {
		panic("sender of sketch did not have reconcil set for receiver")
	}

	need, have := lnSet.Diff(senderSet)
	if len(need)+len(have) <= int(m.(*LightningMessage).DataSize/uint64(InvSize)) {
		ReconcileSuccess++
	} else {
		ReconcileFail++
	}

	// Record set difference
	if len(senderSet.MsgIDs)+len(lnSet.MsgIDs) > 0 {
		RelativeDiffBuckets.Put(uint64((len(need) + len(have)) * 100 / (len(senderSet.MsgIDs) + len(lnSet.MsgIDs))))
	}

	AbsoluteDiffBuckets.Put(uint64(len(need) + len(have)))
	CapacityBuckets.Put(m.(*LightningMessage).DataSize / uint64(InvSize))

	// Send query for needs
	invs := make([]ChannelInv, 0, len(need))
	for _, needID := range need {
		m := ln.LNS.Messages[needID]
		if needID != int(m.ID()) {
			panic("msg id mismatch")
		}

		inv := ChannelInv{
			ChannelID:     m.ChannelID,
			Timestamp:     m.Policy.LastUpdate,
			Direction:     m.Direction,
			IsNotAnUpdate: m.Type != ChannelUpdate,
			MsgID:         m.Id,
		}

		invs = append(invs, inv)
	}

	ln.SendQuery(invs, time, sender)

	// Send invs for have

	haveInvs := make([]ChannelInv, 0, len(have))
	for _, haveID := range have {
		m := ln.LNS.Messages[haveID]
		if haveID != int(m.ID()) {
			panic("msg id mismatch")
		}

		inv := ChannelInv{
			ChannelID:     m.ChannelID,
			Timestamp:     m.Policy.LastUpdate,
			Direction:     m.Direction,
			IsNotAnUpdate: m.Type != ChannelUpdate,
			MsgID:         m.Id,
		}

		haveInvs = append(haveInvs, inv)
	}

	bm := &LightningMessage{
		Type:             ChannelInvs,
		Invs:             haveInvs,
		DataSize:         uint64(1 + InvSize*len(have)),
		BroadcastTime:    time,
		IsReconcilResult: true,
		ReconcilSuccess:  true,
	}

	// Always send the reconciliation success messages
	ln.S.N.SendMessage(
		time, time+2, &ln.S.EQ,
		ln.netNode.ID, sender.NetNode().ID, bm,
	)

	lnSet.Clear()
}

func EstimateCapacity(remoteSetSize, localSetSize int, Q float64) int {
	minDiff := int(math.Abs(float64(remoteSetSize - localSetSize)))
	weightedMin := int(Q * math.Min(float64(remoteSetSize), float64(localSetSize)))
	return minDiff + weightedMin + 1
}

func (ln *LightningNode) HandelReconcilRequest(time uint64, m netsim.Message, sender netsim.Noder) {
	lnSet, ok := ln.ReconcilSets[sender.NetNode().ID]
	if !ok {
		panic("we need a reconcil set for this peer")
	}

	lnm := m.(*LightningMessage)

	c := EstimateCapacity(lnm.ReconcilSetSize, lnSet.Size(), lnm.ReconcilQ)

	bm := &LightningMessage{
		Type:          ReconcilSketch,
		DataSize:      uint64(c * InvSize),
		BroadcastTime: time,
	}

	ln.S.N.SendMessage(
		time, time, &ln.S.EQ,
		ln.netNode.ID, sender.NetNode().ID, bm,
	)
}

// This triggers reconciliation with one of our gossip peers.
// For us the connection is outbound.
func (ln *LightningNode) ReconcileTick(time uint64, t netsim.Ticker) bool {
	peerID := t.Data.(int)
	ticker, ok := ln.GossipTickers[peerID]
	if !ok {
		panic("peer needs to have a ticker")
	}

	ticker.willTick = false
	ticker.lastTicked = time
	ln.GossipTickers[peerID] = ticker

	// Send reconcil request

	m := &LightningMessage{
		Type:            ReconcilRequest,
		DataSize:        uint64(8),
		BroadcastTime:   time,
		ReconcilSetSize: ln.ReconcilSets[peerID].Size(),
		ReconcilQ:       GlobalConfig.Algorithm.ReconcilQ,
	}
	ln.S.N.SendMessage(
		time, time, &ln.S.EQ,
		ln.netNode.ID, peerID, m,
	)

	// Reconcil ticks get rescheduled.
	return false
}

func (ln *LightningNode) ScheduleReconcilTick(currTime uint64, peerID int) {
	t, ok := ln.GossipTickers[peerID]
	if !ok {
		fmt.Println(ln.netNode.ID, peerID, ln.GossipTickers)
		panic("we need to have a ticker to be able to reschedule it")
	}

	if t.willTick {
		return
	}

	nextTick := GetNextTick(currTime, t.lastTicked,
		GlobalConfig.Algorithm.ReconcilTimer*uint64(len(ln.GossipTickers)))
	t.willTick = true
	ln.GossipTickers[peerID] = t

	var _peerID interface{}
	_peerID = t.peerID
	ln.LNS.NS.NewNodeTickerWithData(
		nextTick-currTime,
		0xdeadbeef,
		ln.netNode.ID,
		_peerID,
	)
}

func (ln *LightningNode) ScheduleReconcilTicks(currTime uint64) {
	for id := range ln.GossipTickers {
		ln.ScheduleReconcilTick(currTime, id)
	}

	// Schedule ticks for all our gossip peers.
	// We do this because otherwise the peers don't know when to tick
	// because reconcil ticks don't repeat and we want to sim to stop
	// once messages are done propagating.

	for _, p := range ln.GossipPeers {
		p.(*LightningNode).ScheduleReconcilTick(currTime, ln.netNode.ID)
	}
}

func (ln *LightningNode) AddMessageToReconcilSets(del netsim.Message, add netsim.Message, exclude netsim.Noder) {
	for peerID, set := range ln.ReconcilSets {
		if del.(*LightningMessage) != nil {
			set.Remove(del.(*LightningMessage))
		}

		// Never add a message to the reconcil set of the peer that sent use the message.
		if peerID == exclude.NetNode().ID {
			continue
		}

		set.Add(add.(*LightningMessage))
	}
}
