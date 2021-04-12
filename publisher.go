package eventd

import "context"

// PUBLISHER WAIT LIST

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

func (wl *publisherWaitList) delete(id uint64) {
	delete(wl.idMap, id)
}

// PUBLISHER PROCESSED LIST

type publisherProcessedList struct {
	queue []uint64
	idMap map[uint64]uint64
	last  uint64
	size  uint64
}

func newPublisherProcessedList(limit uint64) *publisherProcessedList {
	return &publisherProcessedList{
		queue: make([]uint64, limit),
		idMap: map[uint64]uint64{},
		last:  0,
		size:  0,
	}
}

func (wl *publisherProcessedList) put(id uint64, seq uint64) {
	wl.idMap[id] = seq
	limit := uint64(len(wl.queue))

	if wl.size == limit {
		delete(wl.idMap, wl.queue[wl.last])
	} else {
		wl.size++
	}

	wl.queue[wl.last] = id
	wl.last = (wl.last + 1) % limit
}

func (wl *publisherProcessedList) get(id uint64) (uint64, bool) {
	seq, ok := wl.idMap[id]
	return seq, ok
}

// PUBLISHER RUNNER

type waitRequest struct {
	eventID      uint64
	responseChan chan<- uint64
}

type publisherRunner struct {
	id        PublisherID
	repo      Repository
	publisher Publisher
	fetchChan chan<- fetchRequest
	respChan  chan fetchResponse
	opts      publisherOpts

	events []Event

	lastSequence uint64
	fetching     bool

	processedList *publisherProcessedList
	waitList      *publisherWaitList
}

func newPublisherRunner(
	id PublisherID, repo Repository, publisher Publisher,
	fetchChan chan<- fetchRequest, opts publisherOpts,
) *publisherRunner {
	return &publisherRunner{
		id:        id,
		repo:      repo,
		publisher: publisher,
		fetchChan: fetchChan,
		respChan:  make(chan fetchResponse, 1),
		opts:      opts,
		events:    make([]Event, 0, opts.publishLimit),

		lastSequence: 0,
		fetching:     false,

		processedList: newPublisherProcessedList(opts.processedListLimit),
		waitList:      newPublisherWaitList(opts.waitListLimit),
	}
}

func (r *publisherRunner) init(ctx context.Context) error {
	seq, err := r.repo.GetLastSequence(ctx, r.id)
	if err != nil {
		return err
	}
	r.lastSequence = seq
	return nil
}

func (r *publisherRunner) isFetching() bool {
	return r.fetching
}

func (r *publisherRunner) fetch() {
	r.fetchChan <- fetchRequest{
		from:         r.lastSequence + 1,
		limit:        r.opts.publishLimit,
		result:       r.events,
		responseChan: r.respChan,
	}
	r.fetching = true
}

func (r *publisherRunner) run(ctx context.Context, waitRequestChan <-chan waitRequest) error {
	select {
	case resp := <-r.respChan:
		// TODO when resp existed is false
		r.fetching = false

		err := r.publisher.Publish(ctx, resp.result)
		if err != nil {
			return err
		}

		for _, e := range resp.result {
			waitRespChan, ok := r.waitList.get(e.ID)
			if ok {
				waitRespChan <- e.Sequence
				r.waitList.delete(e.ID)
			}
			r.processedList.put(e.ID, e.Sequence)
		}

		seq := resp.result[len(resp.result)-1].Sequence
		err = r.repo.SaveLastSequence(ctx, r.id, seq)
		if err != nil {
			return err
		}
		r.lastSequence = seq
		return nil

	case waitReq := <-waitRequestChan:
		seq, ok := r.processedList.get(waitReq.eventID)
		if ok {
			waitReq.responseChan <- seq
		} else {
			r.waitList.put(waitReq.eventID, waitReq.responseChan)
		}
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}
