package lnsim

import (
	"fmt"
	"testing"
)

func TestSimple(t *testing.T) {
	nodes := NewHistoryNodes("./sample.json")
	channels := NewHistoryChannels("./sample.json")
	fmt.Println(nodes)
	fmt.Println(channels)
}
