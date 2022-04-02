package netsim

import (
	"container/heap"
	"log"
)

var LogTimeSteps = uint64(1000 * 10)

type Simulation struct {
	EQ   EventQueue
	N    *Network
	Time uint64
}

func (s *Simulation) Init() {
	s.EQ = make(EventQueue, 0)
	heap.Init(&s.EQ)
}

func (s *Simulation) Start() {
	log.Println("Starting simulation")

	logTime := LogTimeSteps
	numEvents := 0
	for s.EQ.Len() > 0 {
		event := heap.Pop(&s.EQ).(*Event)
		if event.Time < s.Time {
			panic("The time of an event can not be dated in the past")
		}

		s.Time = event.Time

		//log.Println(s.Time)
		if s.Time > logTime {
			size := uint64(0)
			for _, n := range s.N.Nodes {
				size += n.Size()
			}
			log.Printf("Simulated %d events in %03f minutes. (mem usage: %.4fMB nodes, %.4fMB queue)\n", numEvents, float64(s.Time)/1000.0/60.0, float64(size)/float64(1024*1024), float64(s.EQ.Size())/float64(1024*1024))
			logTime = s.Time + LogTimeSteps
		}

		//fmt.Println(reflect.TypeOf(event.Data))
		event.Run(&s.EQ)
		numEvents++
	}

	log.Println("Ending simulation")
}

func (s *Simulation) FlushLogs() {
	s.N.BandwidthLogger.Flush()
	s.N.RedundancyLogger.Flush()
	s.N.InFlightTimeLogger.Flush()
	s.N.BandwidthBuckets.Flush()
}
