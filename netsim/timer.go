package netsim

import (
	"container/heap"
)

type TickerFunc func(data interface{}) bool

type Ticker struct {
	Interval uint64
	Type     int
	N        Noder
	Data     interface{}
	Func     TickerFunc
}

func (s *Simulation) NewNodeTicker(interval uint64, t int, nodeID int, offset uint64) {
	ticker := Ticker{Interval: interval, Type: t, N: s.N.Nodes[nodeID], Func: nil}
	heap.Push(&(s.EQ), &Event{Time: s.Time + interval - offset, Data: ticker})
}

func (s *Simulation) NewNodeTickerWithData(interval uint64, t int, nodeID int, data interface{}) {
	ticker := Ticker{Interval: interval, Type: t, N: s.N.Nodes[nodeID], Func: nil, Data: data}
	heap.Push(&(s.EQ), &Event{Time: s.Time + interval, Data: ticker})
}

func (s *Simulation) NewFuncTicker(interval uint64, t int, offset uint64, fn TickerFunc, data interface{}) {
	ticker := Ticker{Interval: interval, Type: t, N: nil, Func: fn, Data: data}
	heap.Push(&(s.EQ), &Event{Time: s.Time + interval - offset, Data: ticker})
}

func (t Ticker) Run(eq *EventQueue, time uint64) {
	tickAgain := false
	if t.N != nil {
		tickAgain = t.N.Tick(time, t)
	}

	if t.Func != nil {
		tickAgain = t.Func(t.Data)
	}

	if tickAgain {
		heap.Push(eq, &Event{Time: time + t.Interval, Data: t})
	}
}
