package lnsim

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type HistoryNode struct {
	ID string `json:"pub_key"`
}

type HistoryChannel struct {
	Src            string        `json:"node1_pub"`
	Dst            string        `json:"node2_pub"`
	Node1Policy    ChannelPolicy `json:"node1_policy"`
	Node2Policy    ChannelPolicy `json:"node2_policy"`
	StringCapacity string        `json:"capacity"`
	ShortChannelID string        `json:"channel_id"`
}

type HistoryNodes struct {
	Nodes []HistoryNode `json:"nodes"`
}

type HistoryChannels struct {
	Channels []HistoryChannel `json:"edges"`
}

func NewHistoryNodes(path string) *HistoryNodes {
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		panic(err)
	}

	nodes := new(HistoryNodes)
	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(bytes, nodes)
	if err != nil {
		panic(err)
	}

	return nodes
}

func NewHistoryChannels(path string) *HistoryChannels {
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		fmt.Println(path)
		panic(err)
	}

	channels := new(HistoryChannels)
	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(bytes, channels)
	if err != nil {
		panic(err)
	}

	return channels
}
