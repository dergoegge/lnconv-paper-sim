package netsim

import (
	"fmt"
	"math/rand"
	"testing"
)

type TestNode struct {
	netNode   *Node
	BQ        map[uint64]MessageWithSenders
	Seen      map[uint64]int
	S         *Simulation
	ticksLeft int
}

func (tn *TestNode) NetNode() *Node {
	return tn.netNode
}

func (tn *TestNode) Tick(time uint64, t Ticker) bool {
	//	fmt.Println("Node", tn.netNode.ID, "ticked", time, tn.ticksLeft)
	if len(tn.BQ) == 0 {
		goto end
	}

	for _, p := range tn.netNode.Peers {
		for _, m := range tn.BQ {
			_, ok := m.senders[p.NetNode().ID]
			if ok {
				// Dont send messages back to originators
				continue
			}

			tn.S.N.SendMessage(time, &tn.S.EQ, tn.netNode.ID, p.NetNode().ID, m.m)
		}
	}

	tn.BQ = make(map[uint64]MessageWithSenders)

end:
	tn.ticksLeft--
	return tn.ticksLeft > 0
}

func (tn *TestNode) Receive(time uint64, m Message, sender Noder, hops uint64) {
	fmt.Println("Node", tn.netNode.ID, "received msg", time, m.ID())
	if tn.Seen[m.ID()] > 0 {
		return
	}
	tn.Seen[m.ID()]++

	ms, ok := tn.BQ[m.ID()]
	if ok {
		ms.senders[sender.NetNode().ID] = sender
	} else {
		senders := make(map[int]Noder)
		senders[sender.NetNode().ID] = sender
		tn.BQ[m.ID()] = MessageWithSenders{m, senders}
	}

}

type TestMessage struct {
	id uint64
}

type MessageWithSenders struct {
	m       Message
	senders map[int]Noder
}

func (tm *TestMessage) Data() interface{} { return nil }
func (tm *TestMessage) Size() uint64      { return 1 }
func (tm *TestMessage) ID() uint64        { return tm.id }

func TestNetworkSimpleMessage(t *testing.T) {
	sim := Simulation{N: NewNetwork("./test_data"), Time: 0}
	sim.Init()

	/*	sim.N.AddNode(&TestNode{
			netNode: sim.N.NewNode(), ticksLeft: 10, S: &sim,
			BQ: make(map[uint64]MessageWithSenders),
		})
		sim.N.AddNode(&TestNode{
			netNode: sim.N.NewNode(), ticksLeft: 10, S: &sim,
			BQ: make(map[uint64]MessageWithSenders),
		})
		sim.N.AddNode(&TestNode{
			netNode: sim.N.NewNode(), ticksLeft: 10, S: &sim,
			BQ: make(map[uint64]MessageWithSenders),
		})
		sim.N.AddNode(&TestNode{
			netNode: sim.N.NewNode(), ticksLeft: 10, S: &sim,
			BQ: make(map[uint64]MessageWithSenders),
		})

			sim.N.ConnectNodes(0, 1)
			sim.N.ConnectNodes(1, 2)
			sim.N.ConnectNodes(2, 3)
			sim.N.ConnectNodes(3, 0)
			sim.NewTicker(100, 0, 0)
			sim.NewTicker(100, 0, 1)
			sim.NewTicker(100, 0, 2)
			sim.NewTicker(100, 0, 3)*/
	numNodes := 1000
	for i := 0; i < numNodes; i++ {
		sim.N.AddNode(&TestNode{
			netNode: sim.N.NewNode(), ticksLeft: 10000, S: &sim,
			BQ:   make(map[uint64]MessageWithSenders),
			Seen: make(map[uint64]int),
		})
		sim.NewTicker(60, 0, i, uint64(rand.Intn(59)))
	}

	for i := 0; i < numNodes; i++ {
		for j := 0; j < 5; j++ {
			c := i
			for c == i {
				c = rand.Intn(numNodes)
			}
			fmt.Println(i, c)
			sim.N.ConnectNodes(i, c)
		}
	}
	sim.N.SendMessage(sim.Time, &sim.EQ, 0, 1, &TestMessage{id: 69})

	sim.Start()
}
