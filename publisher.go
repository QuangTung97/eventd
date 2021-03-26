package eventd

type publisherWaitList struct {
	queue []uint64
	idMap map[uint64]chan<- uint64
	last  uint64
	size  uint64
}

func newPublisherWaitList(limit uint64) *publisherWaitList {
	return &publisherWaitList{
		queue: make([]uint64, limit),
		idMap: map[uint64]chan<- uint64{},
		last:  0,
		size:  0,
	}
}

func (wl *publisherWaitList) put(id uint64, resp chan<- uint64) {
	wl.idMap[id] = resp
	limit := uint64(len(wl.queue))

	if wl.size == limit {
		delete(wl.idMap, wl.queue[wl.last])
	} else {
		wl.size++
	}

	wl.queue[wl.last] = id
	wl.last = (wl.last + 1) % limit
}

func (wl *publisherWaitList) get(id uint64) (chan<- uint64, bool) {
	ch, ok := wl.idMap[id]
	return ch, ok
}
