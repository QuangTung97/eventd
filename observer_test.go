package eventd

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestObserver_BeforeGetEvents(t *testing.T) {
	fetchCh := make(chan fetchRequest, 1)
	o := newObserver(nil, fetchCh, 40, 5)

	o.beforeGetEvents()
	assert.Equal(t, 1, len(fetchCh))
	req := <-fetchCh
	assert.Equal(t, uint64(40), req.from)
	assert.Equal(t, uint64(5), req.limit)
	assert.Equal(t, 0, len(req.result))
	assert.Equal(t, 5, cap(req.result))
	assert.Equal(t, 1, cap(req.responseChan))
}

func TestObserver_WaitForEvents_Existed(t *testing.T) {
	fetchCh := make(chan fetchRequest, 1)
	repo := &RepositoryMock{}
	o := newObserver(repo, fetchCh, 40, 5)

	o.beforeGetEvents()
	req := <-fetchCh

	req.result = append(req.result, Event{ID: 101, Sequence: 40})
	req.result = append(req.result, Event{ID: 99, Sequence: 41})
	req.result = append(req.result, Event{ID: 94, Sequence: 42})
	req.result = append(req.result, Event{ID: 102, Sequence: 43})

	req.responseChan <- fetchResponse{
		existed: true,
		result:  req.result,
	}

	ctx := newContext()

	events := make([]Event, 0, 8)
	events, err := o.waitForEvents(ctx, events)

	assert.Equal(t, 0, len(req.responseChan))
	assert.Equal(t, nil, err)
	assert.Equal(t, []Event{
		{ID: 101, Sequence: 40},
		{ID: 99, Sequence: 41},
		{ID: 94, Sequence: 42},
		{ID: 102, Sequence: 43},
	}, events)
	assert.Equal(t, 8, cap(events))
}

func TestObserver_WaitForEvents__Context_Cancelled(t *testing.T) {
	fetchCh := make(chan fetchRequest, 1)
	repo := &RepositoryMock{}
	o := newObserver(repo, fetchCh, 40, 5)

	o.beforeGetEvents()

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	events := make([]Event, 0, 8)
	result, err := o.waitForEvents(ctx, events)

	assert.Equal(t, nil, err)
	assert.Equal(t, 8, cap(result))
	assert.Equal(t, context.Canceled, ctx.Err())
}

func TestObserver_WaitForEvents_Not_Existed__GetEventsFrom_Error(t *testing.T) {
	fetchCh := make(chan fetchRequest, 1)
	repo := &RepositoryMock{}

	var loggedErr error
	logger := func(err error) {
		loggedErr = err
	}

	o := newObserver(repo, fetchCh, 40, 5, WithObserverLogger(logger))

	o.beforeGetEvents()
	req := <-fetchCh

	req.responseChan <- fetchResponse{
		existed: false,
		result:  req.result,
	}

	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]Event, error) {
		return nil, errors.New("get-events-from-error")
	}

	ctx := newContext()

	var events []Event
	events, err := o.waitForEvents(ctx, events)

	calls := repo.GetEventsFromCalls()
	assert.Equal(t, 1, len(calls))

	assert.Equal(t, 0, len(req.responseChan))
	assert.Equal(t, errors.New("get-events-from-error"), err)
	assert.Equal(t, errors.New("get-events-from-error"), loggedErr)
	assert.Equal(t, []Event(nil), events)
}

func TestObserver_WaitForEvents_Not_Existed__GetEventsFrom_OK(t *testing.T) {
	fetchCh := make(chan fetchRequest, 1)
	repo := &RepositoryMock{}
	o := newObserver(repo, fetchCh, 40, 5)

	o.beforeGetEvents()
	req := <-fetchCh

	req.responseChan <- fetchResponse{
		existed: false,
		result:  req.result,
	}

	repo.GetEventsFromFunc = func(ctx context.Context, from uint64, limit uint64) ([]Event, error) {
		return []Event{
			{ID: 123, Sequence: 40},
			{ID: 100, Sequence: 41},
			{ID: 190, Sequence: 42},
		}, nil
	}

	ctx := newContext()

	var events []Event
	events, err := o.waitForEvents(ctx, events)

	calls := repo.GetEventsFromCalls()
	assert.Equal(t, 1, len(calls))
	assert.Equal(t, ctx, calls[0].Ctx)
	assert.Equal(t, uint64(40), calls[0].From)

	assert.Equal(t, 0, len(req.responseChan))
	assert.Equal(t, nil, err)
	assert.Equal(t, []Event{
		{ID: 123, Sequence: 40},
		{ID: 100, Sequence: 41},
		{ID: 190, Sequence: 42},
	}, events)
}

func TestObserver_BeforeGetEvents__Second_Times__OK(t *testing.T) {
	fetchCh := make(chan fetchRequest, 1)
	repo := &RepositoryMock{}
	o := newObserver(repo, fetchCh, 40, 5)

	o.beforeGetEvents()
	req := <-fetchCh

	req.responseChan <- fetchResponse{
		existed: true,
		result: []Event{
			{ID: 123, Sequence: 40},
			{ID: 100, Sequence: 41},
			{ID: 190, Sequence: 42},
		},
	}

	ctx := newContext()

	var events []Event
	_, _ = o.waitForEvents(ctx, events)

	o.beforeGetEvents()
	req = <-fetchCh
	assert.Equal(t, uint64(43), req.from)
	assert.Equal(t, uint64(5), req.limit)
	assert.Equal(t, 0, len(req.result))
	assert.Equal(t, 5, cap(req.result))
}
