package lnsim

import (
	"container/heap"
)

type IntHeap []uint64

func (h IntHeap) Len() int {
	return len(h)
}

func (h IntHeap) Less(i, j int) bool {
	return h[i] < h[j]
}

func (h IntHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *IntHeap) Push(x interface{}) {
	*h = append(*h, x.(uint64))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type SortedMap struct {
	keys IntHeap
	M    map[uint64]interface{}
}

func NewSortedMap() *SortedMap {
	return &SortedMap{
		keys: IntHeap{},
		M:    make(map[uint64]interface{}),
	}
}

func (m *SortedMap) Put(key uint64, value interface{}) {
	_, ok := m.Get(key)
	if !ok {
		heap.Push(&m.keys, key)
	}
	m.M[key] = value
}

func (m *SortedMap) Get(key uint64) (interface{}, bool) {
	v, ok := m.M[key]
	return v, ok
}

func (m *SortedMap) Delete(key uint64) {
	if key != m.keys[0] {
		panic("can only remove element 0")
	}

	heap.Pop(&m.keys)
	delete(m.M, key)
}

func (m *SortedMap) Keys() []uint64 {
	return []uint64(m.keys)
}
