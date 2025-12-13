package resource

import "sync"

type node struct {
	mu       sync.RWMutex
	name     string
	kvPairs  []KVPair
	children []node
}

type EntryStorer struct {
	mu   sync.RWMutex
	root node
}

func (s *EntryStorer) ReadBatch(batch *ReadBatch) {
	// TODO
}
