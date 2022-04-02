package netsim

import (
	"container/heap"
	"unsafe"
)

type EventData interface {
	Run(eq *EventQueue, time uint64)
}

type Event struct {
	Time uint64
	Data EventData

	index int
}

func (e *Event) Run(eq *EventQueue) {
	e.Data.Run(eq, e.Time)
}

type EventQueue []*Event

func (eq EventQueue) Len() int { return len(eq) }

func (eq EventQueue) Less(i, j int) bool {
	return eq[i].Time < eq[j].Time
}

func (eq EventQueue) Swap(i, j int) {
	eq[i], eq[j] = eq[j], eq[i]
	eq[i].index = i
	eq[j].index = j
}

func (eq *EventQueue) Push(x interface{}) {
	n := len(*eq)
	event := x.(*Event)
	event.index = n
	*eq = append(*eq, event)
}

func (eq *EventQueue) Pop() interface{} {
	old := *eq
	n := len(old)
	event := old[n-1]
	old[n-1] = nil
	event.index = -1
	*eq = old[0 : n-1]
	return event
}

func (eq *EventQueue) update(event *Event, data EventData, time uint64) {
	event.Data = data
	event.Time = time
	heap.Fix(eq, event.index)
}

func (eq *EventQueue) Size() uint64 {
	return uint64(unsafe.Sizeof(*eq)) + uint64(len(*eq))*uint64(unsafe.Sizeof(Event{}))
}
