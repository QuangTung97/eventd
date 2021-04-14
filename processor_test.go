package eventd

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

type processorTest struct {
	proc *processor
	repo *RepositoryMock
}

func newProcessorTest(opts runnerOpts) *processorTest {
	repo := &RepositoryMock{}
	proc := newProcessor(repo, opts)

	return &processorTest{
		repo: repo,
		proc: proc,
	}
}

func (p *processorTest) initWithEvents(events []Event) {
	p.repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return events, nil
	}
	_ = p.proc.init(context.Background())
}

func newProcessorTestWithEvents(opts runnerOpts, events []Event) *processorTest {
	p := newProcessorTest(opts)
	p.initWithEvents(events)
	return p
}

func TestProcessor_Init__GetLastEvents_Error(t *testing.T) {
	t.Parallel()

	ctx := newContext()
	p := newProcessorTest(defaultRunnerOpts())

	p.repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return nil, errors.New("get-last-events-error")
	}

	err := p.proc.init(ctx)

	calls := p.repo.GetLastEventsCalls()
	assert.Equal(t, 1, len(calls))
	assert.Equal(t, ctx, calls[0].Ctx)
	assert.Equal(t, uint64(1000), calls[0].Limit)
	assert.Equal(t, errors.New("get-last-events-error"), err)
}

func TestProcessor_Init__Without_Last_Events(t *testing.T) {
	t.Parallel()

	ctx := newContext()

	p := newProcessorTest(defaultRunnerOpts())

	p.repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return nil, nil
	}

	err := p.proc.init(ctx)

	calls := p.repo.GetLastEventsCalls()
	assert.Equal(t, 1, len(calls))
	assert.Equal(t, nil, err)
	assert.Equal(t, []Event(nil), p.proc.currentEvents())
}

func TestProcessor_Init__Second_Times_Without_Events(t *testing.T) {
	t.Parallel()

	ctx := newContext()

	p := newProcessorTest(defaultRunnerOpts())

	p.repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return []Event{
			{ID: 100, Sequence: 20},
			{ID: 99, Sequence: 21},
			{ID: 101, Sequence: 22},
		}, nil
	}

	_ = p.proc.init(ctx)

	p.repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return nil, nil
	}
	err := p.proc.init(ctx)

	calls := p.repo.GetLastEventsCalls()
	assert.Equal(t, 2, len(calls))
	assert.Equal(t, nil, err)
	assert.Equal(t, []Event(nil), p.proc.currentEvents())
}

func TestProcessor_Init__With_Last_Events(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
	ctx := context.Background()

	events := []Event{
		{ID: 10, Sequence: 5},
		{ID: 9, Sequence: 6},
		{ID: 11, Sequence: 7},
	}
	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return events, nil
	}

	p := newProcessor(repo, defaultRunnerOpts())
	err := p.init(ctx)

	assert.Equal(t, nil, err)
	assert.Equal(t, events, p.currentEvents())
}

func TestProcessor_Init__With_Last_3_Events__Limit_3(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
	ctx := context.Background()

	events := []Event{
		{ID: 10, Sequence: 5},
		{ID: 9, Sequence: 6},
		{ID: 11, Sequence: 7},
	}
	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return events, nil
	}

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 3,
	})
	err := p.init(ctx)

	assert.Equal(t, uint64(4), repo.GetLastEventsCalls()[0].Limit)
	assert.Equal(t, nil, err)
	assert.Equal(t, events, p.currentEvents())
}

func TestProcessor_Init__With_Last_3_Events__Limit_2(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
	ctx := context.Background()

	events := []Event{
		{ID: 10, Sequence: 5},
		{ID: 9, Sequence: 6},
		{ID: 11, Sequence: 7},
	}
	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return events, nil
	}

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 2,
	})
	err := p.init(ctx)

	assert.Equal(t, nil, err)
	assert.Equal(t, events[1:], p.currentEvents())
}

func TestProcessor_Signal__GetUnprocessed_Error(t *testing.T) {
	t.Parallel()

	ctx := newContext()

	p := newProcessorTestWithEvents(runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 3,
	}, nil)

	p.repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return nil, errors.New("get-unprocessed-error")
	}

	err := p.proc.signal(ctx)

	calls := p.repo.GetUnprocessedEventsCalls()
	assert.Equal(t, 1, len(calls))
	assert.Equal(t, ctx, calls[0].Ctx)
	assert.Equal(t, uint64(4), calls[0].Limit)

	assert.Equal(t, errors.New("get-unprocessed-error"), err)
}

func TestProcessor_Signal__Empty_Unprocessed__Without_Stored(t *testing.T) {
	t.Parallel()

	ctx := newContext()
	p := newProcessorTestWithEvents(runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 3,
	}, nil)

	p.repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return nil, nil
	}

	err := p.proc.signal(ctx)
	assert.Equal(t, nil, err)
	assert.Equal(t, []Event(nil), p.proc.currentEvents())
}

func TestProcessor_Signal__Empty_Unprocessed__With_Stored(t *testing.T) {
	t.Parallel()

	ctx := newContext()
	events := []Event{
		{ID: 10, Sequence: 5},
		{ID: 9, Sequence: 6},
		{ID: 11, Sequence: 7},
	}
	p := newProcessorTestWithEvents(runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 3,
	}, events)

	p.repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return nil, nil
	}

	err := p.proc.signal(ctx)
	assert.Equal(t, nil, err)
	assert.Equal(t, events, p.proc.currentEvents())
}

func TestProcessor_Signal__4_Unprocessed__Update_Error(t *testing.T) {
	t.Parallel()

	ctx := newContext()
	p := newProcessorTestWithEvents(runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 3,
	}, nil)

	p.repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return []Event{
			{ID: 10},
			{ID: 12},
			{ID: 7},
			{ID: 15},
		}, nil
	}

	updatedEvents := []Event{
		{ID: 10, Sequence: 1},
		{ID: 12, Sequence: 2},
		{ID: 7, Sequence: 3},
		{ID: 15, Sequence: 4},
	}
	p.repo.UpdateSequencesFunc = func(ctx context.Context, events []Event) error {
		return errors.New("update-sequences-error")
	}

	err := p.proc.signal(ctx)

	calls := p.repo.UpdateSequencesCalls()
	assert.Equal(t, 1, len(calls))
	assert.Equal(t, updatedEvents, calls[0].Events)

	assert.Equal(t, errors.New("update-sequences-error"), err)
	assert.Equal(t, []Event(nil), p.proc.currentEvents())
}

func TestProcessor_Signal__4_Unprocessed__Update_OK(t *testing.T) {
	t.Parallel()

	ctx := newContext()
	p := newProcessorTestWithEvents(runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 3,
	}, nil)

	p.repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return []Event{
			{ID: 10},
			{ID: 12},
			{ID: 7},
			{ID: 15},
		}, nil
	}

	updatedEvents := []Event{
		{ID: 10, Sequence: 1},
		{ID: 12, Sequence: 2},
		{ID: 7, Sequence: 3},
		{ID: 15, Sequence: 4},
	}
	p.repo.UpdateSequencesFunc = func(ctx context.Context, events []Event) error {
		return nil
	}

	err := p.proc.signal(ctx)

	calls := p.repo.UpdateSequencesCalls()
	assert.Equal(t, 1, len(calls))
	assert.Equal(t, updatedEvents, calls[0].Events)

	assert.Equal(t, nil, err)
	assert.Equal(t, []Event{
		{ID: 12, Sequence: 2},
		{ID: 7, Sequence: 3},
		{ID: 15, Sequence: 4},
	}, p.proc.currentEvents())
	assert.Equal(t, uint64(4), p.proc.getLastSequence())
}

func TestProcessor_Signal__With_Stored_Reach_Limit_5__OK(t *testing.T) {
	t.Parallel()

	ctx := newContext()
	events := []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}

	p := newProcessorTestWithEvents(runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 5,
	}, events)

	p.repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return []Event{
			{ID: 10},
			{ID: 12},
			{ID: 7},
			{ID: 15},
		}, nil
	}

	updatedEvents := []Event{
		{ID: 10, Sequence: 8},
		{ID: 12, Sequence: 9},
		{ID: 7, Sequence: 10},
		{ID: 15, Sequence: 11},
	}
	p.repo.UpdateSequencesFunc = func(ctx context.Context, events []Event) error {
		return nil
	}

	err := p.proc.signal(ctx)

	calls := p.repo.UpdateSequencesCalls()
	assert.Equal(t, 1, len(calls))
	assert.Equal(t, updatedEvents, calls[0].Events)

	assert.Equal(t, nil, err)
	assert.Equal(t, []Event{
		{ID: 5, Sequence: 7},
		{ID: 10, Sequence: 8},
		{ID: 12, Sequence: 9},
		{ID: 7, Sequence: 10},
		{ID: 15, Sequence: 11},
	}, p.proc.currentEvents())
	assert.Equal(t, uint64(11), p.proc.getLastSequence())
}

func TestProcessor_Check_From_Sequence(t *testing.T) {
	t.Parallel()

	events := []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}
	p := newProcessorTestWithEvents(runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 5,
	}, events)

	table := []struct {
		name   string
		from   uint64
		status processorCheckStatus
	}{
		{
			name:   "check-ok",
			from:   7,
			status: processorCheckStatusOK,
		},
		{
			name:   "check-ok",
			from:   5,
			status: processorCheckStatusOK,
		},
		{
			name:   "need-wait",
			from:   8,
			status: processorCheckStatusNeedWait,
		},
		{
			name:   "need-wait",
			from:   9,
			status: processorCheckStatusNeedWait,
		},
		{
			name:   "too-small",
			from:   4,
			status: processorCheckStatusTooSmall,
		},
	}
	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			status := p.proc.checkFromSequence(e.from)
			assert.Equal(t, e.status, status)
		})
	}
}

func TestProcessor_Get_Events_From(t *testing.T) {
	t.Parallel()

	events := []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}
	p := newProcessorTestWithEvents(runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 5,
	}, events)

	assert.Equal(t, uint64(7), p.proc.getLastSequence())

	result := p.proc.getEventsFrom(5, nil, 1)
	assert.Equal(t, []Event{
		{ID: 3, Sequence: 5},
	}, result)

	result = p.proc.getEventsFrom(5, nil, 3)
	assert.Equal(t, []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}, result)

	result = p.proc.getEventsFrom(5, nil, 4)
	assert.Equal(t, []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}, result)
}

func TestProcessor_Run_Context_Cancelled(t *testing.T) {
	t.Parallel()

	p := newProcessorTestWithEvents(runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 5,
	}, nil)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	err := p.proc.run(ctx, nil, nil)
	assert.Nil(t, err)
}

func TestProcessor_Run_Signal(t *testing.T) {
	t.Parallel()

	ctx := newContext()
	events := []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}
	p := newProcessorTestWithEvents(runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 5,
	}, events)

	p.repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return []Event{
			{ID: 10},
			{ID: 12},
			{ID: 7},
			{ID: 15},
		}, nil
	}
	p.repo.UpdateSequencesFunc = func(ctx context.Context, events []Event) error {
		return nil
	}

	updated := []Event{
		{ID: 10, Sequence: 8},
		{ID: 12, Sequence: 9},
		{ID: 7, Sequence: 10},
		{ID: 15, Sequence: 11},
	}

	signals := make(chan struct{}, 1)
	signals <- struct{}{}

	err := p.proc.run(ctx, signals, nil)

	calls := p.repo.UpdateSequencesCalls()
	assert.Equal(t, 1, len(calls))
	assert.Equal(t, updated, calls[0].Events)

	assert.Nil(t, err)
	assert.Equal(t, uint64(11), p.proc.getLastSequence())
	assert.Equal(t, 0, len(signals))
}

func TestProcessor_Run_Multiple_Signals(t *testing.T) {
	t.Parallel()

	ctx := newContext()
	events := []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}
	p := newProcessorTestWithEvents(runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 5,
	}, events)

	p.repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return []Event{
			{ID: 10},
			{ID: 12},
			{ID: 7},
			{ID: 15},
		}, nil
	}
	p.repo.UpdateSequencesFunc = func(ctx context.Context, events []Event) error {
		return nil
	}

	updated := []Event{
		{ID: 10, Sequence: 8},
		{ID: 12, Sequence: 9},
		{ID: 7, Sequence: 10},
		{ID: 15, Sequence: 11},
	}

	signals := make(chan struct{}, 3)
	signals <- struct{}{}
	signals <- struct{}{}
	signals <- struct{}{}

	err := p.proc.run(ctx, signals, nil)

	calls := p.repo.UpdateSequencesCalls()
	assert.Equal(t, 1, len(calls))
	assert.Equal(t, updated, calls[0].Events)

	assert.Nil(t, err)
	assert.Equal(t, uint64(11), p.proc.getLastSequence())
	assert.Equal(t, 0, len(signals))
}

func TestProcessor_Run_Fetch__Not_Existed(t *testing.T) {
	t.Parallel()

	ctx := newContext()
	events := []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}
	p := newProcessorTestWithEvents(runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 5,
	}, events)

	respChan := make(chan fetchResponse, 1)

	fetchChan := make(chan fetchRequest, 1)
	fetchChan <- fetchRequest{
		from:         4,
		limit:        1,
		result:       nil,
		responseChan: respChan,
	}

	err := p.proc.run(ctx, nil, fetchChan)

	assert.Nil(t, err)
	assert.Equal(t, 0, len(fetchChan))
	resp := <-respChan
	assert.Equal(t, fetchResponse{
		existed: false,
	}, resp)
}

func TestProcessor_Run_Fetch__Existed(t *testing.T) {
	t.Parallel()

	ctx := newContext()
	events := []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}
	p := newProcessorTestWithEvents(runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 5,
	}, events)

	respChan := make(chan fetchResponse, 1)

	fetchChan := make(chan fetchRequest, 1)
	fetchChan <- fetchRequest{
		from:         5,
		limit:        2,
		result:       nil,
		responseChan: respChan,
	}

	err := p.proc.run(ctx, nil, fetchChan)

	assert.Nil(t, err)
	assert.Equal(t, 0, len(fetchChan))
	resp := <-respChan
	assert.Equal(t, fetchResponse{
		existed: true,
		result: []Event{
			{ID: 3, Sequence: 5},
			{ID: 6, Sequence: 6},
		},
	}, resp)
}

func TestProcessor_Run_Fetch__Wait(t *testing.T) {
	t.Parallel()

	ctx := newContext()
	events := []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}
	p := newProcessorTestWithEvents(runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 5,
	}, events)

	respChan := make(chan fetchResponse, 3)

	fetchChan := make(chan fetchRequest, 3)
	fetchChan <- fetchRequest{
		from:         8,
		limit:        2,
		result:       nil,
		responseChan: respChan,
	}
	fetchChan <- fetchRequest{
		from:         6,
		limit:        3,
		result:       nil,
		responseChan: respChan,
	}
	fetchChan <- fetchRequest{
		from:         9,
		limit:        3,
		result:       nil,
		responseChan: respChan,
	}

	_ = p.proc.run(ctx, nil, fetchChan)
	_ = p.proc.run(ctx, nil, fetchChan)
	_ = p.proc.run(ctx, nil, fetchChan)

	assert.Equal(t, 0, len(fetchChan))
	assert.Equal(t, 1, len(respChan))

	resp := <-respChan
	assert.Equal(t, fetchResponse{
		existed: true,
		result: []Event{
			{ID: 6, Sequence: 6},
			{ID: 5, Sequence: 7},
		},
	}, resp)

	p.repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return []Event{
			{ID: 10},
			{ID: 12},
		}, nil
	}
	p.repo.UpdateSequencesFunc = func(ctx context.Context, events []Event) error {
		return nil
	}

	signals := make(chan struct{}, 1)
	signals <- struct{}{}
	_ = p.proc.run(ctx, signals, nil)

	assert.Equal(t, 2, len(respChan))

	resp = <-respChan
	assert.Equal(t, fetchResponse{
		existed: true,
		result: []Event{
			{ID: 10, Sequence: 8},
			{ID: 12, Sequence: 9},
		},
	}, resp)

	resp = <-respChan
	assert.Equal(t, fetchResponse{
		existed: true,
		result: []Event{
			{ID: 10, Sequence: 8},
			{ID: 12, Sequence: 9},
		},
	}, resp)
}

func TestProcessor_Run_Fetch__Wait_And_Wait(t *testing.T) {
	t.Parallel()

	ctx := newContext()
	events := []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}
	p := newProcessorTestWithEvents(runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 5,
	}, events)

	respChan := make(chan fetchResponse, 3)
	fetchChan := make(chan fetchRequest, 3)
	signals := make(chan struct{}, 1)

	fetchChan <- fetchRequest{
		from:         8,
		limit:        2,
		result:       nil,
		responseChan: respChan,
	}

	_ = p.proc.run(ctx, signals, fetchChan)
	assert.Equal(t, 0, len(respChan))

	signals <- struct{}{}
	p.repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return []Event{
			{ID: 10},
		}, nil
	}
	p.repo.UpdateSequencesFunc = func(ctx context.Context, events []Event) error {
		return nil
	}

	_ = p.proc.run(ctx, signals, fetchChan)

	resp := <-respChan
	assert.Equal(t, fetchResponse{
		existed: true,
		result: []Event{
			{ID: 10, Sequence: 8},
		},
	}, resp)

	fetchChan <- fetchRequest{
		from:         10,
		limit:        1,
		result:       nil,
		responseChan: respChan,
	}
	fetchChan <- fetchRequest{
		from:         9,
		limit:        3,
		result:       nil,
		responseChan: respChan,
	}

	_ = p.proc.run(ctx, signals, fetchChan)
	_ = p.proc.run(ctx, signals, fetchChan)

	signals <- struct{}{}
	p.repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return []Event{
			{ID: 19},
		}, nil
	}

	_ = p.proc.run(ctx, signals, fetchChan)

	assert.Equal(t, 2, len(respChan))

	resp = <-respChan
	assert.Equal(t, fetchResponse{
		existed: true,
		result: []Event{
			{ID: 19, Sequence: 9},
		},
	}, resp)

	resp = <-respChan
	assert.Equal(t, fetchResponse{
		existed: true,
		result: []Event{
			{ID: 19, Sequence: 9},
		},
	}, resp)
}
