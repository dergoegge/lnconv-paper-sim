package lnsim

import (
	"container/heap"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"

	"git.tu-berlin.de/dergoegge/bachelor_thesis/buckets"
	"git.tu-berlin.de/dergoegge/bachelor_thesis/csvlog"
	"git.tu-berlin.de/dergoegge/bachelor_thesis/netsim"
)

var (
	SeenLogger    *csvlog.CsvLogger
	PaymentLogger *csvlog.CsvLogger
	WaitedLogger  *csvlog.CsvLogger
	ConvBuckets   *buckets.IntBuckets
	WaitedBuckets *buckets.IntBuckets

	// Set reconciliation buckets
	RelativeDiffBuckets *buckets.IntBuckets
	CapacityBuckets     *buckets.IntBuckets
	AbsoluteDiffBuckets *buckets.IntBuckets

	MessageStats map[string]uint64
)

type LightingNetworkSimulation struct {
	NS *netsim.Simulation

	// Graph used for payment finding
	G ChannelGraph

	Messages []*LightningMessage

	NumBlackholes int

	// scid -> sim channel id
	ChannelIDMap map[uint64]int
	// <scid>x<dir> -> sim node id
	EdgeToNode map[string]int
}

func New() *LightingNetworkSimulation {
	dirPath := path.Join(GlobalConfig.OutputDir, "l2")
	_ = os.MkdirAll(dirPath, 0777)

	netSim := netsim.Simulation{
		N:    netsim.NewNetwork(GlobalConfig.OutputDir),
		Time: 0,
	}
	netSim.Init()

	channels := NewHistoryChannels(GlobalConfig.GraphFile).Channels
	nodes := NewHistoryNodes(GlobalConfig.GraphFile).Nodes

	log.Printf("Initializing simulation with %d nodes and %d channels\n",
		len(nodes), len(channels))

	lnSim := new(LightingNetworkSimulation)

	SeenLogger = csvlog.NewCsvLogger(path.Join(GlobalConfig.OutputDir, "l2", "seen.csv"),
		[]string{"timestamp", "node_id", "message_id", "hops"})
	PaymentLogger = csvlog.NewCsvLogger(path.Join(GlobalConfig.OutputDir, "l2", "payments.csv"),
		[]string{"timestamp", "node_src", "node_dst", "amount", "status"})
	WaitedLogger = csvlog.NewCsvLogger(path.Join(GlobalConfig.OutputDir, "l2", "waited.csv"),
		[]string{"timestamp", "node_id", "message_id", "waited", "dropped"})
	ConvBuckets = buckets.NewIntBuckets(10, path.Join(GlobalConfig.OutputDir, "l2", "convbuckets.csv"))
	WaitedBuckets = buckets.NewIntBuckets(100, path.Join(GlobalConfig.OutputDir, "l2", "waitedbuckets.csv"))

	RelativeDiffBuckets = buckets.NewIntBuckets(1, path.Join(GlobalConfig.OutputDir, "l2", "relativediffs.csv"))
	CapacityBuckets = buckets.NewIntBuckets(1, path.Join(GlobalConfig.OutputDir, "l2", "capacities.csv"))
	AbsoluteDiffBuckets = buckets.NewIntBuckets(1, path.Join(GlobalConfig.OutputDir, "l2", "absolutediffs.csv"))

	// map from 32 byte id to local sim net id
	nodeMap := make(map[string]int)
	for i, node := range nodes {
		netNode := netSim.N.NewNode()
		netNode.B = netsim.Bandwidth{Up: 1000000, Down: 1000000}
		lnNode := NewLightningNode(netNode, lnSim, &netSim)
		netSim.N.AddNode(lnNode)
		nodeMap[node.ID] = i
		if GlobalConfig.Algorithm.ReconcilTimer == 0 {
			lnNode.InitStaggerTicker()
		}
	}

	for _, channel := range channels {
		netSim.N.ConnectNodes(nodeMap[channel.Src], nodeMap[channel.Dst])
	}

	// Setup gossip syncers

	if GlobalConfig.Algorithm.SpanningTree {
		// Connect nodes acording to the spanning tree.
		// For now BFS.
		visited := map[int]struct{}{}
		rootNode := 0
		queue := []*LightningNode{netSim.N.Nodes[rootNode].(*LightningNode)}

		visited[rootNode] = struct{}{}

		for len(queue) > 0 {
			// Pop front of queue
			node := queue[0]
			queue = queue[1:]

			// We have to iterate over the peers in sorted order because
			// Go randomizes iteration over maps.
			peerKeys := make([]int, 0, len(node.netNode.Peers))
			for k := range node.netNode.Peers {
				peerKeys = append(peerKeys, k)
			}
			sort.Ints(peerKeys)

			for _, pid := range peerKeys {
				p := netSim.N.Nodes[pid]
				_, ok := visited[p.NetNode().ID]
				if !ok {
					visited[p.NetNode().ID] = struct{}{}
					queue = append(queue, p.(*LightningNode))

					// Connect nodes as active gossip syncers.
					node.SetActiveSyncerForST(p.(*LightningNode))
				}
			}
		}
		log.Println(len(visited), "nodes in BFS spanning tree")

	} else {
		// Make extra outbound connections
		numConnectionsNeeded := 0
		for i, _ := range nodes {
			// Pick n random other nodes to connect to

			// For set reconciliation we want at least 3 outbound conns

			numConns := len(netSim.N.Nodes[i].NetNode().Peers)
			if GlobalConfig.Algorithm.ReconcilTimer > 0 {
				numConns = len(netSim.N.Nodes[i].NetNode().OutboundPeers)
			}
			newConnectionsToMake := int(GlobalConfig.SyncerConnections) - numConns

			if newConnectionsToMake > 0 {
				numConnectionsNeeded++
				//fmt.Println(newConnectionsToMake)
			}

			for j := 0; j < newConnectionsToMake; j++ {
				r := i
				for ; r == i || !netSim.N.ConnectNodes(i, r); r = rand.Intn(len(nodes)) {
				}
			}
			if len(netSim.N.Nodes[i].NetNode().Peers) < int(GlobalConfig.SyncerConnections) {
				panic("not enough peers")
			}
		}
		log.Println(numConnectionsNeeded, "nodes needed additional connections to target of", GlobalConfig.SyncerConnections)

		for i, _ := range nodes {
			ln := netSim.N.Nodes[i].(*LightningNode)
			ln.ChooseActiveSyncers()
		}
	}

	// Create additional gossip blackholes if needed
	numBlackholes := 0
	for i, _ := range nodes {
		ln := netSim.N.Nodes[i].(*LightningNode)
		if ln.IsBlackhole() {
			numBlackholes++
		}
	}
	numBlackholesNeeded := len(nodes) * GlobalConfig.PercentageBlackholes / 100
	numAdditionalBlackholes := numBlackholesNeeded - numBlackholes
	if GlobalConfig.PercentageBlackholes != 0 && numAdditionalBlackholes < 0 {
		log.Println("[WARNING] there will be more blackholes then specified in the config")
	}

	if numAdditionalBlackholes > 0 {
		randIds := rand.Perm(len(nodes))[:numAdditionalBlackholes]
		for _, i := range randIds {
			ln := netSim.N.Nodes[i].(*LightningNode)
			ln.MakeBlackhole()
			numBlackholes++
		}
	}
	log.Println(numBlackholes, "gossip blackholes")

	// Create channel graph for payments

	graph := ChannelGraph{
		Nodes:    make([]GraphNode, 0, len(nodes)),
		Channels: make([]GraphChannel, 0, len(channels)),
	}

	for i := 0; i < len(nodes); i++ {
		graph.Nodes = append(graph.Nodes, GraphNode{
			NetworkID: i,
			Neighbors: make(map[int][]*GraphChannel),
		})
	}

	channelIDMap := make(map[uint64]int)
	edgeToNode := make(map[string]int)
	for i, channel := range channels {
		scid, err := strconv.ParseUint(channel.ShortChannelID, 10, 64)
		if err != nil {
			panic(err)
		}

		channelIDMap[scid] = i
		srcID := nodeMap[channel.Src]
		dstID := nodeMap[channel.Dst]
		capacity, _ := strconv.Atoi(channel.StringCapacity)
		randBalance := rand.Intn(capacity + 1)

		gChannel := GraphChannel{
			ChannelID:    i,
			Node1:        &graph.Nodes[srcID],
			Node2:        &graph.Nodes[dstID],
			Node1Policy:  channel.Node1Policy,
			Node2Policy:  channel.Node2Policy,
			Node1Balance: uint64(randBalance),
			Node2Balance: uint64(capacity) - uint64(randBalance),
			Capacity:     uint64(capacity),
			NewChannel:   false,
		}
		graph.Channels = append(graph.Channels, gChannel)

		edgeToNode[channel.ShortChannelID+"x0"] = srcID
		edgeToNode[channel.ShortChannelID+"x1"] = dstID

		graph.Nodes[srcID].AddChannel(dstID, &graph.Channels[len(graph.Channels)-1])
		graph.Nodes[dstID].AddChannel(srcID, &graph.Channels[len(graph.Channels)-1])
	}

	*lnSim = LightingNetworkSimulation{
		NS:            &netSim,
		G:             graph,
		Messages:      make([]*LightningMessage, 0, int(GlobalConfig.NumberOfMessages)),
		NumBlackholes: numBlackholes,
		ChannelIDMap:  channelIDMap,
		EdgeToNode:    edgeToNode,
	}

	lnSim.PopulateMessages()

	/*for k, v := range channelsInvolved {
		if v > 1 {
			//fmt.Println(k)
		}
	}*/

	amt := uint64(1)
	for i := 0; i < int(GlobalConfig.NumberOfPayments); i++ {
		msPerPayment := int(GlobalConfig.PaymentInterval) / int(GlobalConfig.NumberOfPayments)
		var time uint64 = uint64(i * msPerPayment)

		srcID := rand.Intn(len(netSim.N.Nodes) - 1)
		dstID := rand.Intn(len(netSim.N.Nodes) - 1)
		for srcID == dstID {
			dstID = rand.Intn(len(netSim.N.Nodes) - 1)
		}

		src := netSim.N.Nodes[srcID].(*LightningNode)
		dst := netSim.N.Nodes[dstID].(*LightningNode)

		heap.Push(&netSim.EQ, &netsim.Event{
			Time: time,
			Data: PaymentEvent{
				Src:    src,
				Dst:    dst,
				Amount: amt * 1000, // in msat
			},
		})

		if amt == 100 {
			amt = 1000
		} else if amt == 1000 {
			amt = 10000
		} else if amt == 10000 {
			amt = 100000
		} else if amt == 100000 {
			amt = 100
		}
	}

	log.Println("Injected messages and payment events")

	return lnSim
}

func (lns *LightingNetworkSimulation) PopulateMessages() {
	var replay bool = GlobalConfig.MessageFile != ""
	if replay {
		lns.Messages = ReadMessages(lns, GlobalConfig.MessageFile)
	} else {
		for i := 0; i < int(GlobalConfig.NumberOfMessages); i++ {
			var time uint64 = uint64(rand.Intn(int(GlobalConfig.MessageInterval)+1)) + 1
			m := &LightningMessage{
				Id:            uint64(i),
				DataSize:      uint64(UpdateSize),
				BroadcastTime: time,
				Seen:          1,
			}

			lns.Messages = append(lns.Messages, m)
		}
	}

	// Queue broadcast events
	for _, m := range lns.Messages {
		type broadcastData struct {
			msg    *LightningMessage
			netSim *netsim.Simulation
		}
		//fmt.Println(m.DataSize)

		broadcast := func(data interface{}) bool {
			bd := data.(*broadcastData)

			randNodeID := rand.Intn(len(lns.NS.N.Nodes) - 1)
			lnNode := bd.netSim.N.Nodes[randNodeID].(*LightningNode)

			for !lnNode.BroadcastChannelUpdate(bd.msg, bd.msg.BroadcastTime) {
				randNodeID = rand.Intn(len(lns.NS.N.Nodes) - 1)
				lnNode = lns.NS.N.Nodes[randNodeID].(*LightningNode)
			}

			return false
		}

		if replay {
			broadcast = func(data interface{}) bool {
				bd := data.(*broadcastData)

				lnNode := bd.netSim.N.Nodes[bd.msg.Origin.NetworkID].(*LightningNode)
				lnNode.BroadcastMessage(bd.msg)

				return false
			}
		}

		lns.NS.NewFuncTicker(m.BroadcastTime, 0, 0, broadcast, &broadcastData{
			msg:    m,
			netSim: lns.NS,
		})
	}

}

func (lns *LightingNetworkSimulation) Start() {
	if GlobalConfig.CpuProfile != "" {
		f, err := os.Create(GlobalConfig.CpuProfile)
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f)
	}

	MessageStats = make(map[string]uint64)
	netsim.MessageHook = func(m netsim.Message) {
		t := m.(*LightningMessage).Type
		tBytes, _ := MessageStats[t.String()]
		tBytes += m.Size()
		MessageStats[t.String()] = tBytes
	}

	lns.NS.Start()

	for k, v := range MessageStats {
		log.Println(k, v)
	}

	if GlobalConfig.Algorithm.ReconcilTimer > 0 {
		log.Println("reconcil success:", float64(ReconcileSuccess)/float64(ReconcileSuccess+ReconcileFail))
		log.Println("reconcil fail:", float64(ReconcileFail)/float64(ReconcileSuccess+ReconcileFail))
	}

	// Log redundancy
	redunBucket := buckets.NewIntBuckets(1, path.Join(GlobalConfig.OutputDir, "l2", "redun.csv"))
	for _, node := range lns.NS.N.Nodes {
		n := node.(*LightningNode)
		for _, meta := range n.Seen {
			redunBucket.Put(uint64(meta.Count))
		}
	}
	redunBucket.Flush()

	lns.NS.FlushLogs()

	SeenLogger.Flush()
	PaymentLogger.Flush()
	WaitedLogger.Flush()
	lns.NS.N.RedundancyLogger.Flush()
	ConvBuckets.Flush()
	WaitedBuckets.Flush()
	RelativeDiffBuckets.Flush()
	AbsoluteDiffBuckets.Flush()
	CapacityBuckets.Flush()

	if GlobalConfig.MemProfile != "" {
		f, err := os.Create(GlobalConfig.MemProfile)
		if err != nil {
			fmt.Println(err)
		}
		runtime.GC()
		pprof.WriteHeapProfile(f)
	}

	if GlobalConfig.CpuProfile != "" {
		pprof.StopCPUProfile()
	}

	// Sanity checks
	numMessages := 0
	buckets := make([]int, 10)
	for _, m := range lns.Messages {
		bucketSize := len(lns.NS.N.Nodes) / len(buckets)
		buckets[int(math.Min(float64(len(buckets)-1), float64(int(m.Seen)/bucketSize)))]++
		if int(m.Seen) != len(lns.NS.N.Nodes) {

			numMessages++
			//fmt.Printf("message for channel %d not propagated to all nodes %d != %d\n",
			//		m.ChannelID, m.Seen, len(lns.NS.N.Nodes))
		}
	}
	for i := range buckets {
		if i > 0 {
			buckets[i] = buckets[i-1] + buckets[i]
		}
	}

	for i, b := range buckets {
		log.Printf("<%d%% - %d\n", (i+1)*(100/len(buckets)), b)
	}
	log.Println(numMessages, "messages did not make it to all nodes")

	nodesWithUndels := 0
	for _, node := range lns.NS.N.Nodes {
		lnNode := node.(*LightningNode)
		if len(lnNode.Seen) != 0 {
			nodesWithUndels++
			//fmt.Printf("%d messages were not deleted from node %d\n",
			//	len(lnNode.Seen), node.NetNode().ID)
		}
		//fmt.Println(lnNode.Seen)
	}
	log.Println(nodesWithUndels, "nodes still had messages in their maps")
}
