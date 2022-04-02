package lnsim

import (
	"encoding/json"
	"fmt"
	"math"
)

type ChannelWeight uint64

const InfinityWeight = math.MaxUint64

func (w ChannelWeight) IsInfinity() bool {
	return w == InfinityWeight
}

func (w ChannelWeight) Sub(o ChannelWeight) ChannelWeight {
	if w.IsInfinity() || o.IsInfinity() {
		return InfinityWeight
	}

	if o > w {
		return 0
	}

	return w - o
}

func (w ChannelWeight) Add(o ChannelWeight) ChannelWeight {
	if w.IsInfinity() || o.IsInfinity() {
		return InfinityWeight
	}

	return w + o
}

type ChannelPolicy struct {
	TimeLockDelta    uint16  `json:"time_lock_delta"`
	MinHtlc          string  `json:"min_htlc"`
	MaxHtlcMsat      string  `json:"max_htlc_msat"`
	FeeBaseMsat      float64 `json:"fee_base_msat,string"`
	FeeRateMilliMsat float64 `json:"fee_rate_milli_msat,string"`
	Disabled         bool    `json:"disabled"`
	LastUpdate       uint64  `json:"last_update"`
}

func (cp ChannelPolicy) IsDisruptive(other ChannelPolicy) bool {
	return cp.FeeRateMilliMsat < other.FeeRateMilliMsat ||
		cp.FeeBaseMsat < other.FeeBaseMsat ||
		cp.TimeLockDelta < other.TimeLockDelta
}

func (cp ChannelPolicy) IsNonDisruptive(other ChannelPolicy) bool {
	return cp.FeeRateMilliMsat > other.FeeRateMilliMsat ||
		cp.FeeBaseMsat > other.FeeBaseMsat ||
		cp.TimeLockDelta > other.TimeLockDelta
}

func (cp ChannelPolicy) IsKeepalive(other ChannelPolicy) bool {
	return cp.LastUpdate < other.LastUpdate &&
		cp.TimeLockDelta == other.TimeLockDelta &&
		cp.MinHtlc == other.MinHtlc &&
		cp.MaxHtlcMsat == other.MaxHtlcMsat &&
		cp.FeeBaseMsat == other.FeeBaseMsat &&
		cp.FeeRateMilliMsat == other.FeeRateMilliMsat &&
		cp.Disabled == other.Disabled
}

func (cp ChannelPolicy) Compare(other ChannelPolicy) int {
	// 0: keep alive
	// 1: channel close
	// 2: channel re-open
	// 3: disruptive
	// 4: non-disruptive
	// 5: misc

	if cp.IsKeepalive(other) {
		return 0
	}

	if !cp.Disabled && other.Disabled {
		return 1
	}

	if cp.Disabled && !other.Disabled {
		return 2
	}

	if cp.IsDisruptive(other) {
		return 3
	}

	if cp.IsNonDisruptive(other) {
		return 4
	}

	return 5
}

func (cp *ChannelPolicy) GetFee(amt uint64) uint64 {
	base := uint64(cp.FeeBaseMsat)
	rate := uint64(cp.FeeRateMilliMsat)
	return base + (rate * amt / 1000)
}

func (p *ChannelPolicy) UnmarshalJSON(text []byte) error {
	type policy ChannelPolicy
	pol := policy{
		Disabled: false,
		//		LastUpdate: 0,
		// TODO: default values
	}

	if err := json.Unmarshal(text, &pol); err != nil {
		return err
	}
	if pol.LastUpdate == 0 {
		// These edges are weird but appear in the graph file.
		// Porbably saw a channel_announcement but no channel_updates.
		// Avoid routing through them.
		pol.Disabled = true
	}

	pol.LastUpdate *= 1000

	*p = ChannelPolicy(pol)
	return nil
}

type GraphChannel struct {
	ChannelID int

	Node1, Node2 *GraphNode

	Node1Policy ChannelPolicy
	Node2Policy ChannelPolicy

	Node1Balance, Node2Balance uint64

	Capacity uint64

	// Whether this channel was announced during the simulation.
	NewChannel bool

	FromNode bool
}

func (c *GraphChannel) UpdatePolicy(dir int, newPolicy ChannelPolicy) {
	if dir == 0 {
		c.Node1Policy = newPolicy
	} else if dir == 1 {
		c.Node2Policy = newPolicy
	} else {
		panic("UpdatePolicy dir must be 0 or 1")
	}
}

func (c *GraphChannel) IsDisabled(n *GraphNode) bool {
	if n == c.Node1 {
		return c.Node1Policy.Disabled
	} else if n == c.Node2 {
		return c.Node2Policy.Disabled
	} else {
		panic("IsDisabled: n must be one of the endpoints")
	}

	return false
}

func (c *GraphChannel) GetBalance(n *GraphNode) uint64 {
	if n == c.Node1 {
		return c.Node1Balance
	} else if n == c.Node2 {
		return c.Node2Balance
	} else {
		panic("GetBalance: n must be one of the endpoints")
	}

	return 0
}

func (c *GraphChannel) GetPolicyByDir(dir int) ChannelPolicy {
	if dir == 0 {
		return c.Node1Policy
	} else if dir == 1 {
		return c.Node2Policy
	} else {
		panic("GetTimeLockDelta: n must be one of the endpoints")
	}
}

// returns the outgoing policy
func (c *GraphChannel) GetPolicy(n *GraphNode) ChannelPolicy {
	if n == c.Node1 {
		return c.Node1Policy
	} else if n == c.Node2 {
		return c.Node2Policy
	} else {
		panic("GetTimeLockDelta: n must be one of the endpoints")
	}
}

func (c *GraphChannel) GetTimeLockDelta(n *GraphNode) uint16 {
	if n == c.Node1 {
		return c.Node1Policy.TimeLockDelta
	} else if n == c.Node2 {
		return c.Node2Policy.TimeLockDelta
	} else {
		panic("GetTimeLockDelta: n must be one of the endpoints")
	}
}

func (c *GraphChannel) GetFee(amt uint64, n *GraphNode) uint64 {
	if c == nil {
		panic("GetFee: channel is nil")
	}

	var base uint64
	var rate uint64

	if n == c.Node1 {
		base = uint64(c.Node1Policy.FeeBaseMsat)
		rate = uint64(c.Node1Policy.FeeRateMilliMsat)
	} else if n == c.Node2 {
		base = uint64(c.Node2Policy.FeeBaseMsat)
		rate = uint64(c.Node2Policy.FeeRateMilliMsat)
	} else {
		panic("GetFee: n must be one of the endpoints")
	}

	return base + (amt * rate / 1000)
}

// Returns the direction (index) of the outgoing edge of the node.
func (c *GraphChannel) GetDirection(n *GraphNode) int {
	if n == c.Node1 {
		return 0
	} else if n == c.Node2 {
		return 1
	} else {
		panic("GetDirection node not a channel partner")
	}
}

type GraphNode struct {
	// neighborID -> channels
	Neighbors map[int][]*GraphChannel
	// All channels of this node
	Channels []*GraphChannel

	// The ID of the node in the netsim.Network
	NetworkID int
}

func (n *GraphNode) AddChannel(neighbor int, c *GraphChannel) {
	if neighbor == n.NetworkID {
		panic("neighbor cant be the node itself")
	}

	n.Neighbors[neighbor] = append(n.Neighbors[neighbor], c)
	n.Channels = append(n.Channels, c)
}

type ChannelGraph struct {
	Nodes    []GraphNode
	Channels []GraphChannel
}

type PaymentPath struct {
	Src, Dst *GraphNode
	Nodes    []*GraphNode
	Hops     []*GraphChannel

	AggWeight ChannelWeight
	AggAmt    uint64
	AggTime   uint16

	Amounts []uint64
	Times   []uint16
}

func (p PaymentPath) String() string {
	str := "A"
	node := 'B'
	current := p.Src
	for i, c := range p.Hops {
		fee := uint64(0)
		if i < len(p.Amounts)-1 {
			fee = p.Amounts[i] - p.Amounts[i+1]
		}
		str = fmt.Sprintf("%s --(amt: %d, lock: %d)--> %c(fee: %d)", str, p.Amounts[i], p.Times[i], node, fee)
		fmt.Println(c.Node1Policy, c.Node2Policy)
		if current == c.Node1 {
			current = c.Node2
		} else if current == c.Node2 {
			current = c.Node1
		} else {
			panic("path not connected")
		}
		node++
	}

	return str
}

func (p PaymentPath) Copy() (ret PaymentPath) {
	nodesCopy := make([]*GraphNode, len(p.Nodes))
	copy(nodesCopy, p.Nodes)
	hopsCopy := make([]*GraphChannel, len(p.Hops))
	copy(hopsCopy, p.Hops)
	ret = p
	ret.Nodes = nodesCopy
	ret.Hops = hopsCopy
	return
}

func (p *PaymentPath) AddHop(c *GraphChannel, aggAmount uint64, aggTime uint16) {
	p.Amounts = append([]uint64{aggAmount}, p.Amounts...)
	p.Times = append([]uint16{aggTime}, p.Times...)

	if len(p.Hops) == 0 {
		p.Hops = append(p.Hops, c)
		return
	}

	front := p.Hops[0]
	if front.Node1 == c.Node1 ||
		front.Node1 == c.Node2 ||
		front.Node2 == c.Node1 ||
		front.Node2 == c.Node2 {
		// Channel connects
		p.Hops = append([]*GraphChannel{c}, p.Hops...)
		return
	}

	panic("channel did not connect")
}

func NewPaymentPath(amt uint64, src, dst *GraphNode) PaymentPath {
	return PaymentPath{
		Src:       src,
		Dst:       dst,
		Nodes:     make([]*GraphNode, 0),
		Hops:      make([]*GraphChannel, 0),
		AggWeight: ChannelWeight(0),
		AggAmt:    amt,
		AggTime:   0,
	}
}

// edgeWeight computes the weight of an edge. This value is used when searching
// for the shortest path within the channel graph between two nodes. Weight is
// is the fee itself plus a time lock penalty added to it. This benefits
// channels with shorter time lock deltas and shorter (hops) routes in general.
// RiskFactor controls the influence of time lock on route selection. This is
// currently a fixed value, but might be configurable in the future.
func edgeWeight(lockedAmt uint64, fee uint64, timeLockDelta uint16) ChannelWeight {
	// timeLockPenalty is the penalty for the time lock delta of this channel.
	// It is controlled by RiskFactorBillionths and scales proportional
	// to the amount that will pass through channel. Rationale is that it if
	// a twice as large amount gets locked up, it is twice as bad.
	timeLockPenalty := lockedAmt * uint64(timeLockDelta) *
		/*RiskFactorBillionths*/ 15 / 1000000000

	return ChannelWeight(fee).Add(ChannelWeight(timeLockPenalty))
}

func NextToVisit(distanceMap *SortedMap) *GraphNode {
	var next *GraphNode = nil
	if len(distanceMap.Keys()) == 0 {
		return next
	}

	w := distanceMap.Keys()[0]
	li, ok := distanceMap.Get(w)
	if !ok {
		panic("key not in map")
	}

	l := li.([]*GraphNode)
	if len(l) == 0 {
		panic("list in distance map should not be empty here")
	}

	// Pop first node from list
	next = l[0]
	l = l[1:]

	distanceMap.Put(w, l)

	if len(l) == 0 {
		// Delete weight from distance map if list is empty
		distanceMap.Delete(w)
	}

	return next
}

func FindCheapestEdge(maxTimeLock uint16, currentNode, toNode, srcNode *GraphNode, srcNetNode *LightningNode, candidateMap map[int]PaymentPath) (ChannelWeight, *GraphChannel) {
	candidatePath, ok := candidateMap[currentNode.NetworkID]
	if !ok {
		return InfinityWeight, nil
	}

	if currentNode == srcNode {
		return InfinityWeight, nil
	}

	amount := candidatePath.AggAmt
	currMaxTimeLock := maxTimeLock - candidatePath.AggTime

	candidateEdges := currentNode.Neighbors[toNode.NetworkID]
	var cheapestPrice ChannelWeight = InfinityWeight
	var cheapestEdge *GraphChannel = nil

	for i := range candidateEdges {
		edge := candidateEdges[i]

		// ==============================================================================
		// Get edge from network node (srcNetNode).
		// If a channel_update is stored on the node then use it for the edge policy.
		// If not use the edge from the base graph.
		channelMessages, ok := srcNetNode.SeenByChannel[edge.ChannelID]
		if ok {
			dir := edge.GetDirection(toNode)
			if channelMessages[dir] == nil && edge.NewChannel {
				// Channel was announced but update was not seen yet.
				// The network node cant route over this edge.
				continue
			} else if channelMessages[dir] == nil && !edge.NewChannel {
				// Use existing edge
			} else if channelMessages[dir] != nil {
				// Use this channel_update for the edge
				edgeCopy := *edge

				if channelMessages[dir].Origin != toNode {
					fmt.Println(channelMessages[dir].Origin == toNode, channelMessages[dir].Origin == currentNode)
					panic("FindCheapestEdge channelMessages[dir].Origin != currentNode")
				}

				edgeCopy.UpdatePolicy(dir, channelMessages[dir].Policy)
				edge = &edgeCopy
				edge.FromNode = true
			}
		} else if edge.NewChannel {
			// Channel was announced but the node has not seen that
			continue
		}
		// ==============================================================================

		if edge.IsDisabled(toNode) {
			continue
		}

		fee := edge.GetFee(amount, toNode)
		if amount+fee > edge.Capacity*1000 {
			continue
		}

		if currentNode == srcNode || toNode == srcNode {
			balance := edge.GetBalance(toNode)
			if amount+fee > balance*1000 {
				continue
			}
		}

		timeLockDelta := edge.GetTimeLockDelta(toNode)
		if currMaxTimeLock < timeLockDelta {
			continue
		}

		weight := edgeWeight(amount, fee, timeLockDelta)
		if weight < cheapestPrice {
			cheapestPrice = weight
			cheapestEdge = edge
		}
	}

	/*if cheapestEdge != nil && cheapestEdge.ChannelID == 4567 {
		fmt.Println("cheapest edge:")
		fmt.Println("channel", cheapestEdge.ChannelID)
		fmt.Println("edge", cheapestEdge)
		fmt.Println("srcNode", srcNode.NetworkID)
		fmt.Println("currentNode", currentNode.NetworkID)
		fmt.Println("toNode", toNode.NetworkID)
	}*/

	return cheapestPrice, cheapestEdge
}

func FindPaymentPath(src, dst int, amount uint64, maxTimeLockDelta uint16, lightningSim *LightingNetworkSimulation) PaymentPath {
	graph := lightningSim.G
	srcNode := &(graph.Nodes[src])
	srcNetNode := lightningSim.NS.N.Nodes[srcNode.NetworkID].(*LightningNode)
	dstNode := &(graph.Nodes[dst])
	currentNode := dstNode

	if src == dst {
		return NewPaymentPath(0, srcNode, dstNode)
	}

	visited := make(map[int]*GraphNode)
	//distanceMap := make(map[ChannelWeight][]*GraphNode)
	distanceMap := NewSortedMap()
	candidateMap := make(map[int]PaymentPath)

	distanceMap.Put(0, []*GraphNode{&(graph.Nodes[currentNode.NetworkID])})
	candidateMap[currentNode.NetworkID] = NewPaymentPath(amount, srcNode, dstNode)

	for currentNode = NextToVisit(distanceMap); currentNode != nil; currentNode = NextToVisit(distanceMap) {
		_, ok := visited[currentNode.NetworkID]
		if ok {
			continue
		}

		currentCandidate := candidateMap[currentNode.NetworkID].Copy()

		for neighborID, _ := range currentNode.Neighbors {
			toNode := &(graph.Nodes[neighborID])
			cheapestWeight, cheapestEdge := FindCheapestEdge(
				maxTimeLockDelta,
				currentNode,
				toNode,
				srcNode,
				srcNetNode,
				candidateMap,
			)

			if cheapestEdge == nil {
				continue
			}

			cheapestFee := cheapestEdge.GetFee(currentCandidate.AggAmt, toNode)
			cheapestTimeLock := cheapestEdge.GetTimeLockDelta(toNode)

			oldNeighborCandidate, ok := candidateMap[neighborID]
			if ok && cheapestWeight.Add(currentCandidate.AggWeight) >= oldNeighborCandidate.AggWeight {
				// if we have an old value, only update if new weight is lower than old.
				continue
			}

			newPath := currentCandidate.Copy()
			newPath.AddHop(cheapestEdge, newPath.AggAmt, newPath.AggTime)
			newPath.AggWeight = newPath.AggWeight.Add(cheapestWeight)
			newPath.AggAmt = newPath.AggAmt + cheapestFee
			newPath.AggTime = newPath.AggTime + cheapestTimeLock
			if newPath.AggAmt > cheapestEdge.Capacity*1000 {
				panic("https://git.tu-berlin.de/rohrer/cdt-data/-/blob/master/simulator/lnsim/src/pathfind.rs#L124")
			}

			candidateMap[neighborID] = newPath

			li, ok := distanceMap.Get(uint64(newPath.AggWeight))
			if ok {
				//distanceMap[newPath.AggWeight] = append(distanceMap[newPath.AggWeight], &graph.Nodes[neighborID])
				l := li.([]*GraphNode)
				distanceMap.Put(uint64(newPath.AggWeight), append(l, &graph.Nodes[neighborID]))
			} else {
				//distanceMap[newPath.AggWeight] = []*GraphNode{&(graph.Nodes[neighborID])}
				distanceMap.Put(uint64(newPath.AggWeight), []*GraphNode{&(graph.Nodes[neighborID])})
			}
		}

		if currentNode == srcNode {
			return candidateMap[srcNode.NetworkID].Copy()
		}

		visited[currentNode.NetworkID] = currentNode
	}

	// This can be reached if there is no path of channels with the required capacity
	// to carry the payment.
	return NewPaymentPath(0, srcNode, dstNode)
}
