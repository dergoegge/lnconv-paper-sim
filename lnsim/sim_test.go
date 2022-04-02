package lnsim

import (
	"math/rand"
	"testing"
)

func TestSimSimple(t *testing.T) {
	sim := New(Inputs{
		Channels: "lngraph_2020_11_08__02_00.json",
		Nodes:    "lngraph_2020_11_08__02_00.json",
		DataDir:  "test_data",
	})

	for i := 0; i < 1000; i++ {
		var time uint64 = uint64(rand.Intn(1000 * 60 * 60))
		sim.NS.N.SendMessage(time, &sim.NS.EQ, 0, rand.Intn(len(sim.NS.N.Nodes)-1)+1, &LightningMessage{id: uint64(i), size: 1, creationTime: time})
	}

	sim.NS.Start()
	sim.SeenLogger.Flush()
}
