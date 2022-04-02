package netsim

import (
	"container/heap"
	"math"
	"os"
	"path"
	"strconv"

	"git.tu-berlin.de/dergoegge/bachelor_thesis/buckets"
	"git.tu-berlin.de/dergoegge/bachelor_thesis/csvlog"
)

// Should simulate bandwidth usage?
var SimulateBandwidth bool = true

// Should log message inflight times?
var LogInflight bool = false

// Should log bandwidth?
var LogBandwidth bool = true

// Log total bandwidth usage in intervals of:
var LogBandwidthInterval uint64 = 1000 // in ms

// Bandwidth represents the bandwidth limits of a node.
// The unit is bytes/sec.
type Bandwidth struct {
	Up   uint64
	Down uint64
}

type Noder interface {
	NetNode() *Node
	Receive(time uint64, m Message, sender Noder, hops uint64)
	Tick(time uint64, t Ticker) bool
	Cleanup(m Message)
	Size() uint64
}

type Node struct {
	ID int

	N *Network

	Peers         map[int]Noder
	InboundPeers  map[int]Noder
	OutboundPeers map[int]Noder

	MQ []InFlightMessage

	// Bandwidth restrictions of this node
	B Bandwidth
	// Total bandwidth usage of this node
	BandwidthUsage Bandwidth

	NumIncommingMessages uint64
	NumIncommingBytes    uint64
	NumOutgoingMessages  uint64
	NumOutgoingBytes     uint64
}

func (n *Network) NewNode() *Node {
	return &Node{
		N: n,

		Peers:         make(map[int]Noder),
		OutboundPeers: make(map[int]Noder),
		InboundPeers:  make(map[int]Noder),
	}
}

type Message interface {
	Data() interface{}
	Size() uint64
	ID() uint64
	TimesSeen() uint64
}

type InFlightMessage struct {
	M Message

	TimeAdded       uint64
	BytesDownloaded uint64
	Sender          Noder
}

type MessageSendAtEvent struct {
	ME          MessageEvent
	ArrivalTime uint64
}

func (me MessageSendAtEvent) Run(eq *EventQueue, time uint64) {
	PushMessageEvent(eq, me.ME, time)
	n := me.ME.sender.NetNode().N
	lastTimeLogged := n.LastimeBandwidthLogged
	if LogBandwidth && time-lastTimeLogged >= LogBandwidthInterval {
		n.BandwidthLogger.MustLog([]string{
			strconv.FormatUint(time, 10),
			strconv.FormatUint(n.TotalBandwidth, 10),
		})
		n.LastimeBandwidthLogged = time
	}

	n.BandwidthBuckets.PutCount(time, n.TotalBandwidth-n.LastBandwidthLogged)
	n.LastBandwidthLogged = n.TotalBandwidth
}

type MessageEvent struct {
	m        Message
	sender   Noder
	receiver Noder
	// how many hops has this message already traveled.
	hops uint64
}

func (me MessageEvent) Run(eq *EventQueue, time uint64) {
	me.receiver.NetNode().BandwidthUsage.Down += me.m.Size()
	me.receiver.NetNode().NumIncommingBytes -= me.m.Size()
	me.receiver.NetNode().NumIncommingMessages--

	me.sender.NetNode().NumOutgoingMessages -= me.m.Size()
	me.sender.NetNode().NumOutgoingMessages--

	me.receiver.Receive(time, me.m, me.sender, me.hops)
}

type Network struct {
	Nodes                  []Noder
	TotalBandwidth         uint64
	LastimeBandwidthLogged uint64

	// last value of TotalBandwidth when we wrote it to the buckets.
	LastBandwidthLogged uint64

	BandwidthBuckets *buckets.IntBuckets

	BandwidthLogger    *csvlog.CsvLogger
	RedundancyLogger   *csvlog.CsvLogger
	InFlightTimeLogger *csvlog.CsvLogger
}

func NewNetwork(dir string) *Network {
	dirPath := path.Join(dir, "l1")
	_ = os.MkdirAll(dirPath, 0777)

	return &Network{
		Nodes:              make([]Noder, 0),
		BandwidthBuckets:   buckets.NewIntBuckets(10*1000, path.Join(dirPath, "bandwidth_buckets.csv")),
		BandwidthLogger:    csvlog.NewCsvLogger(path.Join(dirPath, "bandwidth.csv"), []string{"timestamp", "total_bandwidth"}),
		RedundancyLogger:   csvlog.NewCsvLogger(path.Join(dirPath, "redundancy.csv"), []string{"num_seen"}),
		InFlightTimeLogger: csvlog.NewCsvLogger(path.Join(dirPath, "inflighttime.csv"), []string{"timestamp", "ms_in_flight"}),
	}
}

func (n *Network) DeleteMessage(m Message) {
	for _, node := range n.Nodes {
		node.Cleanup(m)
	}
}

func (n *Network) AddNode(node Noder) {
	node.NetNode().ID = len(n.Nodes)
	n.Nodes = append(n.Nodes, node)
}

func (n *Network) ConnectNodes(n1, n2 int) bool {
	node1 := n.Nodes[n1]
	node2 := n.Nodes[n2]

	_, ok1 := node1.NetNode().Peers[n2]
	_, ok2 := node2.NetNode().Peers[n1]
	if ok1 || ok2 {
		// Already connected
		return false
	}

	node1.NetNode().OutboundPeers[n2] = node2
	node2.NetNode().InboundPeers[n1] = node1

	node1.NetNode().Peers[n2] = node2
	node2.NetNode().Peers[n1] = node1

	return true
}

func CalcArrivalTime(senderBand, receiverBand Bandwidth, time, bytesOut, bytesIn, messageSize uint64) uint64 {
	fixedOverhead := uint64(100)
	if !SimulateBandwidth {
		// Instant message transmission
		return time + fixedOverhead
	}

	// TODO: figure out good latency overhead
	band := math.Min(float64(senderBand.Up), float64(receiverBand.Down))

	timeInSecs := float64(messageSize+bytesIn) / band
	timeInMs := uint64(timeInSecs * 1000)
	return time + timeInMs + fixedOverhead
}

type MessageHookFn = func(m Message)

var MessageHook MessageHookFn = nil

func PushMessageEvent(eq *EventQueue, me MessageEvent, time uint64) {
	// Calculate the time of message arrival at n2 based n1's and n2's bandwidth.
	arrivalTime := CalcArrivalTime(me.sender.NetNode().B, me.receiver.NetNode().B, time,
		me.sender.NetNode().NumOutgoingBytes, me.receiver.NetNode().NumIncommingBytes, me.m.Size())

	me.sender.NetNode().N.TotalBandwidth += me.m.Size()

	// ==========================
	if MessageHook != nil {
		MessageHook(me.m)
	}
	// ==========================

	me.receiver.NetNode().NumIncommingBytes += me.m.Size()
	me.receiver.NetNode().NumIncommingMessages++

	me.sender.NetNode().BandwidthUsage.Up += me.m.Size()
	me.sender.NetNode().NumOutgoingBytes += me.m.Size()
	me.sender.NetNode().NumOutgoingMessages++

	me.sender.NetNode().N.InFlightTimeLogger.Log([]string{
		strconv.FormatUint(time, 10),
		strconv.Itoa(int(arrivalTime - time)),
	})

	heap.Push(eq, &Event{Time: arrivalTime, Data: me})
}

// TODO: differentiate between send at time and arrive at time
func (n *Network) SendMessageWithHops(currTime, time uint64, eq *EventQueue, from, to int, m Message, hops uint64) {
	if from == to {
		panic("cant send message to your self")
	}

	nodeFrom := n.Nodes[from]
	nodeTo := n.Nodes[to]

	// Add an event to the queue that fires on message arrival at n2
	me := MessageEvent{m: m, receiver: nodeTo, sender: nodeFrom, hops: hops + 1}
	if currTime == time {
		PushMessageEvent(eq, me, time)

		lastTimeLogged := nodeFrom.NetNode().N.LastimeBandwidthLogged
		if LogBandwidth && time-lastTimeLogged >= LogBandwidthInterval {
			n.BandwidthLogger.MustLog([]string{
				strconv.FormatUint(time, 10),
				strconv.FormatUint(n.TotalBandwidth, 10),
			})
		}

		n.BandwidthBuckets.PutCount(currTime, n.TotalBandwidth-n.LastBandwidthLogged)
		n.LastBandwidthLogged = n.TotalBandwidth

	} else {
		// Send message at a later time
		heap.Push(eq, &Event{Time: time, Data: MessageSendAtEvent{ME: me}})
	}
}

func (n *Network) SendMessage(currTime, time uint64, eq *EventQueue, from, to int, m Message) {
	n.SendMessageWithHops(currTime, time, eq, from, to, m, 0)
}
