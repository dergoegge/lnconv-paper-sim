package netsim

import (
	"container/heap"
	"testing"
)

type TestEvent string

func (e TestEvent) Run(eq *EventQueue, time uint64) {}

func TestSimple(t *testing.T) {
	eq := make(EventQueue, 0)
	heap.Init(&eq)
	heap.Push(&eq, &Event{Time: 100, Data: TestEvent("2")})
	heap.Push(&eq, &Event{Time: 10, Data: TestEvent("1")})
	heap.Push(&eq, &Event{Time: 100000, Data: TestEvent("5")})
	heap.Push(&eq, &Event{Time: 1000, Data: TestEvent("4")})
	heap.Push(&eq, &Event{Time: 500, Data: TestEvent("3")})

	var lastTime uint64 = 0
	for eq.Len() > 0 {
		event := heap.Pop(&eq).(*Event)
		if event.Time < lastTime {
			t.Fail()
		}
	}
}
