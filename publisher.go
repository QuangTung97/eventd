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
	opts      publisherOpts
	repo      Repository
	publisher Publisher

	fetchChan       chan<- fetchRequest
	waitRequestChan chan waitRequest

	respChan chan fetchResponse
	events   []Event

	lastSequence uint64
	fetching     bool

	processedList *publisherProcessedList
	waitList      *publisherWaitList
}

func newPublisherRunner(
	id PublisherID, repo Repository, publisher Publisher,
	fetchChan chan<- fetchRequest,
	waitRequestChan chan waitRequest,
	opts publisherOpts,
) *publisherRunner {
	return &publisherRunner{
		id:        id,
		opts:      opts,
		repo:      repo,
		publisher: publisher,

		fetchChan:       fetchChan,
		waitRequestChan: waitRequestChan,

		respChan: make(chan fetchResponse, 1),
		events:   make([]Event, 0, opts.publishLimit),
	}
}

func (r *publisherRunner) init(ctx context.Context) error {
	r.lastSequence = 0
	r.fetching = false
	r.processedList = newPublisherProcessedList(r.opts.processedListLimit)
	r.waitList = newPublisherWaitList(r.opts.waitListLimit)

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

func (r *publisherRunner) fetch(ctx context.Context) {
	req := fetchRequest{
		from:         r.lastSequence + 1,
		limit:        r.opts.publishLimit,
		result:       r.events,
		responseChan: r.respChan,
	}
	select {
	case r.fetchChan <- req:
		r.fetching = true
		return
	case <-ctx.Done():
		return
	}
}

func (r *publisherRunner) handleFetchResponse(ctx context.Context, resp fetchResponse) error {
	r.fetching = false

	events := resp.result
	if !resp.existed {
		gotEvents, err := r.repo.GetEventsFrom(ctx, r.lastSequence+1, r.opts.publishLimit)
		if err != nil {
			return err
		}
		events = gotEvents
	}
	if len(events) == 0 {
		return ErrEventsNotFound
	}

	published := make([]Event, len(events))
	copy(published, events)
	err := r.publisher.Publish(ctx, published)
	if err != nil {
		return err
	}

	for _, e := range events {
		waitRespChan, ok := r.waitList.get(e.ID)
		if ok {
			waitRespChan <- e.Sequence
			r.waitList.delete(e.ID)
		}
		r.processedList.put(e.ID, e.Sequence)
	}

	seq := events[len(events)-1].Sequence
	err = r.repo.SaveLastSequence(ctx, r.id, seq)
	if err != nil {
		return err
	}
	r.lastSequence = seq
	return nil
}

func (r *publisherRunner) run(ctx context.Context) error {
	select {
	case resp := <-r.respChan:
		return r.handleFetchResponse(ctx, resp)

	case waitReq := <-r.waitRequestChan:
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
