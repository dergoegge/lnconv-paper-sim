package lnsim

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"git.tu-berlin.de/dergoegge/bachelor_thesis/buckets"
)

func CategorizeMessages(graph *ChannelGraph, messages []*LightningMessage) {
	bucketSizeInMs := 60000 * 10
	buckets := [13]*buckets.IntBuckets{
		buckets.NewIntBuckets(uint64(bucketSizeInMs), path.Join(GlobalConfig.OutputDir, "l2", "keepalive.csv")),
		buckets.NewIntBuckets(uint64(bucketSizeInMs), path.Join(GlobalConfig.OutputDir, "l2", "closures.csv")),
		buckets.NewIntBuckets(uint64(bucketSizeInMs), path.Join(GlobalConfig.OutputDir, "l2", "reopens.csv")),
		buckets.NewIntBuckets(uint64(bucketSizeInMs), path.Join(GlobalConfig.OutputDir, "l2", "disruptive.csv")),
		buckets.NewIntBuckets(uint64(bucketSizeInMs), path.Join(GlobalConfig.OutputDir, "l2", "nondisruptive.csv")),
		buckets.NewIntBuckets(uint64(bucketSizeInMs), path.Join(GlobalConfig.OutputDir, "l2", "misc.csv")),
		buckets.NewIntBuckets(uint64(bucketSizeInMs), path.Join(GlobalConfig.OutputDir, "l2", "node_announcements.csv")),
		buckets.NewIntBuckets(uint64(bucketSizeInMs), path.Join(GlobalConfig.OutputDir, "l2", "channel_announcements.csv")),
		buckets.NewIntBuckets(uint64(bucketSizeInMs), path.Join(GlobalConfig.OutputDir, "l2", "channel_updates.csv")),
		buckets.NewIntBuckets(5000, path.Join(GlobalConfig.OutputDir, "l2", "keepalive_times.csv")),
		// timestamp was before the time seen
		buckets.NewIntBuckets(60000, path.Join(GlobalConfig.OutputDir, "l2", "timeseen_vs_timestamp_past.csv")),
		// timestamp was after the time seen
		buckets.NewIntBuckets(60000, path.Join(GlobalConfig.OutputDir, "l2", "timeseen_vs_timestamp_future.csv")),
		buckets.NewIntBuckets(5000, path.Join(GlobalConfig.OutputDir, "l2", "reopendelays.csv")),
	}

	channels := make(map[int][2]*LightningMessage)

	for _, m := range messages {
		switch m.Type {
		case ChannelUpdate:
			channel, ok := channels[m.ChannelID]
			var previousPolicy ChannelPolicy
			fromGraph := true
			if ok {
				edge := channel[m.Direction]
				if edge == nil {
					previousPolicy = graph.Channels[m.ChannelID].GetPolicyByDir(m.Direction)
				} else {
					previousPolicy = edge.Policy
					fromGraph = false
				}
			} else {
				channel = [2]*LightningMessage{nil, nil}
				previousPolicy = graph.Channels[m.ChannelID].GetPolicyByDir(m.Direction)
			}
			t := previousPolicy.Compare(m.Policy)
			buckets[t].Put(m.BroadcastTime)

			buckets[8].Put(m.BroadcastTime)

			m.IsKeepalive = t == 0

			if !m.RandomSCID && fromGraph && t == 0 && previousPolicy.LastUpdate != 0 {
				//fmt.Println(m.Policy.LastUpdate, previousPolicy.LastUpdate)
				buckets[9].Put(m.Policy.LastUpdate - previousPolicy.LastUpdate)
			}

			if !m.RandomSCID && t == 2 && previousPolicy.LastUpdate != 0 {
				if m.Policy.LastUpdate > previousPolicy.LastUpdate {
					buckets[12].Put(m.Policy.LastUpdate - previousPolicy.LastUpdate)
				} else {
					//fmt.Println("skip")
				}
			}

			channel[m.Direction] = m
			channels[m.ChannelID] = channel

			//fmt.Println(m.CreationTime, m.TimeSeen)
			if int64(m.Policy.LastUpdate)-int64(m.BroadcastTime) >= 0 {
				//buckets[11].Put(m.Policy.LastUpdate - m.BroadcastTime)
			} else {
				//buckets[10].Put(m.BroadcastTime - m.Policy.LastUpdate)
			}

		case ChannelAnnouncement:
			buckets[7].Put(m.BroadcastTime)
		case NodeAnnouncement:
			buckets[6].Put(m.BroadcastTime)
		default:
		}
	}

	for _, b := range buckets {
		b.Flush()
	}
	log.Println("Categorized", len(messages), "messages for", len(channels), "channels")
}

func ReadMessages(lns *LightingNetworkSimulation, path string) []*LightningMessage {
	log.Println("Parsing messages from", path)
	f, _ := os.Open(path)

	r := csv.NewReader(f)
	minTime := uint64(time.Now().Unix()) * 1000

	skippedMessages := 0
	allMessages := 0
	messages := []*LightningMessage{}
	lines := 0
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		lines++
		if lines == 1 {
			continue
		}

		// type,timestamp,channel,channel_flags,cltv_expiry_delta,htlc_minimum_msat,fee_base_msat,fee_proportional_millionths

		m := &LightningMessage{}
		allMessages++

		nTime, err := strconv.ParseUint(record[1], 10, 64)
		var time uint64
		if err != nil {
			fTime, err := strconv.ParseFloat(record[1], 64)
			if err != nil {
				panic(err)
			}

			time = uint64(fTime * 1000)
		} else {
			time = uint64(nTime * 1000)
		}

		if time == 0 {
			panic(record)
		}

		if time < GlobalConfig.StartTime*1000 || time > GlobalConfig.EndTime*1000 {
			//continue
		}

		channelID, _ := strconv.ParseUint(record[2], 10, 64)

		// Get sim channel id
		simChannelID, ok := lns.ChannelIDMap[channelID]
		if !ok {
			simChannelID = rand.Intn(len(lns.G.Channels))
			m.RandomSCID = true
		}

		simChannel := &lns.G.Channels[simChannelID]

		channelFlags, _ := strconv.Atoi(record[3])

		// Get origin node
		m.Direction = channelFlags & 1
		origin := simChannel.Node1
		if m.Direction == 1 {
			origin = simChannel.Node2
		}

		m.ChannelID = simChannelID
		m.Origin = origin

		fTimeSeen, err := strconv.ParseFloat(record[10], 64)
		if err != nil {
			panic(err)
		}
		m.BroadcastTime = uint64(fTimeSeen * 1000)
		if m.BroadcastTime < minTime {
			minTime = m.BroadcastTime
		}
		//fmt.Println(record[11], record[12])
		if strings.Contains(record[0], "announcement") {
			m.Type = NodeAnnouncement
			if record[0] == "channel_announcement" {
				m.Type = ChannelAnnouncement
			}

			m.BroadcastTime = time
			messages = append(messages, m)
			continue
		}

		timeLockDelta, _ := strconv.ParseUint(record[4], 10, 16)
		minHtlc := record[5]
		maxHtlc := record[6]
		baseFee, _ := strconv.ParseUint(record[7], 10, 64)
		feeRate, _ := strconv.ParseUint(record[8], 10, 64)

		m.Type = ChannelUpdate
		m.Policy = ChannelPolicy{
			LastUpdate:       time, // updated in second loop
			Disabled:         channelFlags&0b10 == 0b10,
			TimeLockDelta:    uint16(timeLockDelta),
			FeeBaseMsat:      float64(baseFee),
			FeeRateMilliMsat: float64(feeRate),
			MinHtlc:          minHtlc,
			MaxHtlcMsat:      maxHtlc,
		}
		messages = append(messages, m)
	}

	if len(messages) == 0 {
		panic("no messages were generated")
	}

	log.Printf("skipped %d/%d messages\n", skippedMessages, allMessages)
	log.Printf("Min time: %d\n", minTime)

	sort.Slice(messages, func(i, j int) bool {
		// announcements don't have a policy but we don't care about their order here,
		// since they will be inserted into the simulation by their broadcast time.
		return messages[i].Policy.LastUpdate < messages[j].Policy.LastUpdate
	})

	// Second pass to update timestamps
	for i, m := range messages {
		m.Id = uint64(i)
		m.BroadcastTime -= minTime
		//m.Policy.LastUpdate -= minTime
	}

	CategorizeMessages(&lns.G, messages)

	sortedByBroadcastTime := make([]*LightningMessage, len(messages))
	copy(sortedByBroadcastTime, messages)
	sort.Slice(sortedByBroadcastTime, func(i, j int) bool {
		return sortedByBroadcastTime[i].BroadcastTime < sortedByBroadcastTime[j].BroadcastTime
	})

	firstMsgIndex := sort.Search(len(sortedByBroadcastTime), func(i int) bool {
		return sortedByBroadcastTime[i].BroadcastTime >= GlobalConfig.StartTime*1000
	})
	lastMsgIndex := sort.Search(len(sortedByBroadcastTime), func(i int) bool {
		return sortedByBroadcastTime[i].BroadcastTime >= GlobalConfig.EndTime*1000
	})

	fmt.Println(lastMsgIndex, len(sortedByBroadcastTime))
	if lastMsgIndex < len(sortedByBroadcastTime) && firstMsgIndex < len(sortedByBroadcastTime) {
		sortedByBroadcastTime = sortedByBroadcastTime[firstMsgIndex:lastMsgIndex]
	}

	maxTime := sortedByBroadcastTime[len(sortedByBroadcastTime)-1].BroadcastTime
	log.Println("Last message", maxTime+minTime)
	log.Println("Message interval:", (maxTime)/1000/60, "min")
	log.Println("num messages: ", len(sortedByBroadcastTime))
	for i, m := range sortedByBroadcastTime {
		m.Id = uint64(i)
	}
	return sortedByBroadcastTime
}
