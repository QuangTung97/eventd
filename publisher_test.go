package eventd

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

// PUBLISHER WAIT LIST

func TestPublisherWaitList_Single(t *testing.T) {
	wl := newPublisherWaitList(1)
	var expected chan<- uint64

	wait1 := make(chan uint64)
	wl.put(20, wait1)

	resp, ok := wl.get(20)

	assert.True(t, ok)
	expected = wait1
	assert.Equal(t, expected, resp)
}

func TestPublisherWaitList_Double(t *testing.T) {
	wl := newPublisherWaitList(2)
	var expected chan<- uint64

	wait1 := make(chan uint64)
	wait2 := make(chan uint64)
	wl.put(20, wait1)
	wl.put(40, wait2)

	resp, ok := wl.get(20)
	assert.True(t, ok)
	expected = wait1
	assert.Equal(t, expected, resp)

	resp, ok = wl.get(40)
	assert.True(t, ok)
	expected = wait2
	assert.Equal(t, expected, resp)

	resp, ok = wl.get(5)
	assert.False(t, ok)
	expected = nil
	assert.Equal(t, expected, resp)
}

func TestPublisherWaitList_Exceeded(t *testing.T) {
	wl := newPublisherWaitList(2)
	var expected chan<- uint64

	wait1 := make(chan uint64)
	wait2 := make(chan uint64)
	wait3 := make(chan uint64)
	wl.put(20, wait1)
	wl.put(40, wait2)
	wl.put(50, wait3)

	resp, ok := wl.get(20)
	assert.False(t, ok)
	expected = nil
	assert.Equal(t, expected, resp)

	resp, ok = wl.get(40)
	assert.True(t, ok)
	expected = wait2
	assert.Equal(t, expected, resp)
}

func TestPublisherWaitList_Delete(t *testing.T) {
	wl := newPublisherWaitList(2)
	var expected chan<- uint64

	wait1 := make(chan uint64)
	wait2 := make(chan uint64)
	wait3 := make(chan uint64)
	wl.put(20, wait1)
	wl.put(40, wait2)

	wl.delete(20)

	resp, ok := wl.get(20)
	assert.False(t, ok)
	expected = nil
	assert.Equal(t, expected, resp)

	wl.put(50, wait3)

	resp, ok = wl.get(40)
	assert.True(t, ok)
	expected = wait2
	assert.Equal(t, expected, resp)

	resp, ok = wl.get(50)
	assert.True(t, ok)
	expected = wait3
	assert.Equal(t, expected, resp)
}

// PUBLISHER PROCESSED LIST
func TestPublisherProcessedList_Single(t *testing.T) {
	wl := newPublisherProcessedList(1)
	wl.put(20, 100)

	resp, ok := wl.get(20)

	assert.True(t, ok)
	assert.Equal(t, uint64(100), resp)
}

func TestPublisherProcessedList_Double(t *testing.T) {
	wl := newPublisherProcessedList(2)

	wl.put(20, 100)
	wl.put(40, 200)

	resp, ok := wl.get(20)
	assert.True(t, ok)
	assert.Equal(t, uint64(100), resp)

	resp, ok = wl.get(40)
	assert.True(t, ok)
	assert.Equal(t, uint64(200), resp)

	resp, ok = wl.get(5)
	assert.False(t, ok)
	assert.Equal(t, uint64(0), resp)
}

func TestPublisherProcessedList_Exceeded(t *testing.T) {
	wl := newPublisherProcessedList(2)

	wl.put(20, 100)
	wl.put(40, 200)
	wl.put(50, 300)

	resp, ok := wl.get(20)
	assert.False(t, ok)
	assert.Equal(t, uint64(0), resp)

	resp, ok = wl.get(40)
	assert.True(t, ok)
	assert.Equal(t, uint64(200), resp)
}

// PUBLISHER RUNNER

type publisherTest struct {
	repo      *RepositoryMock
	publisher *PublisherMock
	runner    *publisherRunner
}

func newPublisherTest(id PublisherID, fetchChan chan<- fetchRequest,
	options publisherOpts,
) *publisherTest {
	repo := &RepositoryMock{}
	publisher := &PublisherMock{}
	runner := newPublisherRunner(id, repo, publisher, fetchChan, options)
	return &publisherTest{
		repo:      repo,
		publisher: publisher,
		runner:    runner,
	}
}

func (p *publisherTest) initWithLastSequence(seq uint64) {
	p.repo.GetLastSequenceFunc = func(ctx context.Context, id PublisherID) (uint64, error) {
		return seq, nil
	}
	_ = p.runner.init(context.Background())
}

func (p *publisherTest) stubPublishing() {
	p.publisher.PublishFunc = func(ctx context.Context, events []Event) error {
		return nil
	}
	p.repo.SaveLastSequenceFunc = func(ctx context.Context, id PublisherID, seq uint64) error {
		return nil
	}
}

func newContext() context.Context {
	return context.Background()
}

func TestPublisherRunner_Init_Error(t *testing.T) {
	ctx := newContext()
	p := newPublisherTest(5, nil, publisherOpts{
		waitListLimit: 3,
	})

	p.repo.GetLastSequenceFunc = func(ctx context.Context, id PublisherID) (uint64, error) {
		return 0, errors.New("get-last-seq-error")
	}

	err := p.runner.init(ctx)

	calls := p.repo.GetLastSequenceCalls()
	assert.Equal(t, 1, len(calls))
	assert.Equal(t, ctx, calls[0].Ctx)
	assert.Equal(t, PublisherID(5), calls[0].ID)

	assert.Equal(t, errors.New("get-last-seq-error"), err)
	assert.False(t, p.runner.isFetching())
}

func TestPublisherRunner_Fetch(t *testing.T) {
	fetchChan := make(chan fetchRequest, 1)
	p := newPublisherTest(5, fetchChan, publisherOpts{
		waitListLimit: 3,
		publishLimit:  5,
	})

	p.repo.GetLastSequenceFunc = func(ctx context.Context, id PublisherID) (uint64, error) {
		return 50, nil
	}

	err := p.runner.init(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, 1, len(p.repo.GetLastSequenceCalls()))

	p.runner.fetch()

	assert.Equal(t, 1, len(fetchChan))
	req := <-fetchChan
	assert.Equal(t, uint64(51), req.from)
	assert.Equal(t, uint64(5), req.limit)
	assert.Equal(t, 5, cap(req.result))
	assert.Equal(t, 0, len(req.result))
	assert.NotNil(t, req.responseChan)
	assert.True(t, p.runner.isFetching())
}

func TestPublisherRunner_Run__Processor_Response_Not_Existed__GetEventsFrom_Error(t *testing.T) {
	fetchChan := make(chan fetchRequest, 1)
	p := newPublisherTest(5, fetchChan, publisherOpts{
		waitListLimit: 3,
		publishLimit:  5,
	})
	ctx := newContext()

	p.initWithLastSequence(50)

	p.runner.fetch()

	req := <-fetchChan
	req.responseChan <- fetchResponse{
		existed: false,
	}

	p.repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]Event, error) {
		return nil, errors.New("get-events-from-error")
	}

	err := p.runner.run(ctx, nil)

	calls := p.repo.GetEventsFromCalls()
	assert.Equal(t, 1, len(calls))
	assert.Equal(t, ctx, calls[0].Ctx)
	assert.Equal(t, uint64(51), calls[0].From)
	assert.Equal(t, uint64(5), calls[0].Limit)

	assert.Equal(t, errors.New("get-events-from-error"), err)
}

func TestPublisherRunner_Run__Processor_Response_Not_Existed__GetEventsFrom_Empty(t *testing.T) {
	fetchChan := make(chan fetchRequest, 1)
	p := newPublisherTest(5, fetchChan, publisherOpts{
		waitListLimit: 3,
		publishLimit:  5,
	})
	ctx := newContext()

	p.initWithLastSequence(50)

	p.runner.fetch()

	req := <-fetchChan
	req.responseChan <- fetchResponse{
		existed: false,
	}

	p.repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]Event, error) {
		return nil, nil
	}

	err := p.runner.run(ctx, nil)

	calls := p.repo.GetEventsFromCalls()
	assert.Equal(t, 1, len(calls))
	assert.Equal(t, ctx, calls[0].Ctx)
	assert.Equal(t, uint64(51), calls[0].From)
	assert.Equal(t, uint64(5), calls[0].Limit)

	assert.Equal(t, ErrEventsNotFound, err)
}

func TestPublisherRunner_Run__Processor_Response_Not_Existed__Publish_Error(t *testing.T) {
	fetchChan := make(chan fetchRequest, 1)
	p := newPublisherTest(5, fetchChan, publisherOpts{
		waitListLimit: 3,
		publishLimit:  5,
	})
	ctx := newContext()

	p.initWithLastSequence(50)

	p.runner.fetch()

	req := <-fetchChan
	req.responseChan <- fetchResponse{
		existed: false,
	}

	p.repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]Event, error) {
		return []Event{
			{ID: 102, Sequence: 51},
			{ID: 100, Sequence: 52},
			{ID: 105, Sequence: 53},
		}, nil
	}
	p.publisher.PublishFunc = func(ctx context.Context, events []Event) error {
		return errors.New("publish-error")
	}

	err := p.runner.run(ctx, nil)

	assert.Equal(t, 1, len(p.repo.GetEventsFromCalls()))

	calls := p.publisher.PublishCalls()
	assert.Equal(t, 1, len(calls))
	assert.Equal(t, ctx, calls[0].Ctx)
	assert.Equal(t, []Event{
		{ID: 102, Sequence: 51},
		{ID: 100, Sequence: 52},
		{ID: 105, Sequence: 53},
	}, calls[0].Events)

	assert.Equal(t, errors.New("publish-error"), err)
}

func TestPublisherRunner_Run__Processor_Response_Not_Existed__Publish_OK(t *testing.T) {
	fetchChan := make(chan fetchRequest, 1)
	p := newPublisherTest(9, fetchChan, publisherOpts{
		processedListLimit: 4,
		waitListLimit:      3,
		publishLimit:       5,
	})
	ctx := newContext()

	p.initWithLastSequence(50)

	p.runner.fetch()

	req := <-fetchChan
	req.responseChan <- fetchResponse{
		existed: false,
	}

	p.repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]Event, error) {
		return []Event{
			{ID: 102, Sequence: 51},
			{ID: 100, Sequence: 52},
			{ID: 105, Sequence: 53},
		}, nil
	}
	p.publisher.PublishFunc = func(ctx context.Context, events []Event) error {
		return nil
	}
	p.repo.SaveLastSequenceFunc = func(ctx context.Context, id PublisherID, seq uint64) error {
		return nil
	}

	err := p.runner.run(ctx, nil)

	assert.Equal(t, 1, len(p.repo.GetEventsFromCalls()))
	assert.Equal(t, 1, len(p.publisher.PublishCalls()))

	calls := p.repo.SaveLastSequenceCalls()
	assert.Equal(t, 1, len(calls))
	assert.Equal(t, ctx, calls[0].Ctx)
	assert.Equal(t, PublisherID(9), calls[0].ID)
	assert.Equal(t, uint64(53), calls[0].Seq)

	assert.Equal(t, nil, err)
	assert.Equal(t, uint64(53), p.runner.lastSequence)
}

func TestPublisherRunner_Run__Publish_Error(t *testing.T) {
	fetchChan := make(chan fetchRequest, 1)
	p := newPublisherTest(
		5, fetchChan,
		publisherOpts{
			waitListLimit: 3,
			publishLimit:  5,
		},
	)
	p.initWithLastSequence(50)
	ctx := newContext()

	p.runner.fetch()

	req := <-fetchChan
	req.responseChan <- fetchResponse{
		existed: true,
		result: []Event{
			{ID: 5, Sequence: 20},
			{ID: 3, Sequence: 21},
		},
	}

	p.publisher.PublishFunc = func(ctx context.Context, events []Event) error {
		return errors.New("publish-error")
	}

	published := []Event{
		{ID: 5, Sequence: 20},
		{ID: 3, Sequence: 21},
	}

	err := p.runner.run(ctx, nil)
	assert.Equal(t, errors.New("publish-error"), err)

	calls := p.publisher.PublishCalls()
	assert.Equal(t, 1, len(calls))
	assert.Equal(t, ctx, calls[0].Ctx)
	assert.Equal(t, published, calls[0].Events)

	assert.Equal(t, uint64(50), p.runner.lastSequence)
}

func TestPublisherRunner_Run__Save_Last_Seq_Error(t *testing.T) {
	fetchChan := make(chan fetchRequest, 1)
	p := newPublisherTest(
		11, fetchChan,
		publisherOpts{
			processedListLimit: 4,
			waitListLimit:      3,
			publishLimit:       5,
		})
	p.initWithLastSequence(13)

	ctx := newContext()

	p.runner.fetch()
	req := <-fetchChan
	req.responseChan <- fetchResponse{
		existed: true,
		result: []Event{
			{ID: 5, Sequence: 20},
			{ID: 3, Sequence: 21},
		},
	}
	p.publisher.PublishFunc = func(ctx context.Context, events []Event) error {
		return nil
	}

	p.repo.SaveLastSequenceFunc = func(ctx context.Context, id PublisherID, seq uint64) error {
		return errors.New("save-last-seq-error")
	}

	err := p.runner.run(ctx, nil)

	calls := p.repo.SaveLastSequenceCalls()
	assert.Equal(t, 1, len(calls))
	assert.Equal(t, PublisherID(11), calls[0].ID)
	assert.Equal(t, uint64(21), calls[0].Seq)

	assert.Equal(t, errors.New("save-last-seq-error"), err)
	assert.Equal(t, uint64(13), p.runner.lastSequence)
}

func TestPublisherRunner_Run__Context_Cancelled(t *testing.T) {
	fetchChan := make(chan fetchRequest, 1)
	p := newPublisherTest(
		11, fetchChan,
		publisherOpts{
			waitListLimit: 3,
			publishLimit:  5,
		})
	p.initWithLastSequence(50)

	p.runner.fetch()

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	err := p.runner.run(ctx, nil)
	assert.Equal(t, context.Canceled, err)
}

func TestPublisherRunner_Run__Response_To_Wait__After_Recv_Resp_From_Processor(t *testing.T) {
	fetchChan := make(chan fetchRequest, 1)
	p := newPublisherTest(
		11, fetchChan,
		publisherOpts{
			processedListLimit: 4,
			waitListLimit:      3,
			publishLimit:       5,
		})
	p.initWithLastSequence(50)
	p.stubPublishing()
	ctx := newContext()

	p.runner.fetch()

	waitRespChan := make(chan uint64, 1)
	waitReqChan := make(chan waitRequest, 1)
	waitReqChan <- waitRequest{
		eventID:      3,
		responseChan: waitRespChan,
	}

	_ = p.runner.run(ctx, waitReqChan)

	req := <-fetchChan
	req.responseChan <- fetchResponse{
		existed: true,
		result: []Event{
			{ID: 5, Sequence: 20},
			{ID: 3, Sequence: 21},
		},
	}

	err := p.runner.run(ctx, waitReqChan)

	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(waitReqChan))
	assert.Equal(t, 1, len(waitRespChan))
	resp := <-waitRespChan
	assert.Equal(t, uint64(21), resp)

	assert.Equal(t, uint64(21), p.runner.lastSequence)
	assert.False(t, p.runner.isFetching())
}

func TestPublisherRunner_Run__Response_To_Wait__Right_After_Wait_Request(t *testing.T) {
	fetchChan := make(chan fetchRequest, 1)
	p := newPublisherTest(
		11, fetchChan,
		publisherOpts{
			processedListLimit: 4,
			waitListLimit:      3,
			publishLimit:       5,
		})
	p.initWithLastSequence(50)
	p.stubPublishing()
	ctx := newContext()

	p.runner.fetch()

	req := <-fetchChan
	req.responseChan <- fetchResponse{
		existed: true,
		result: []Event{
			{ID: 5, Sequence: 20},
			{ID: 3, Sequence: 21},
		},
	}

	err := p.runner.run(ctx, nil)

	waitRespChan := make(chan uint64, 1)
	waitReqChan := make(chan waitRequest, 1)
	waitReqChan <- waitRequest{
		eventID:      3,
		responseChan: waitRespChan,
	}

	_ = p.runner.run(ctx, waitReqChan)

	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(waitReqChan))
	assert.Equal(t, 1, len(waitRespChan))
	resp := <-waitRespChan
	assert.Equal(t, uint64(21), resp)

	assert.Equal(t, uint64(21), p.runner.lastSequence)
	assert.False(t, p.runner.isFetching())
}
