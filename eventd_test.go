package eventd

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestRunner__GetLastEvents_Error(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
	r := New(repo, WithErrorSleepDuration(30*time.Millisecond))

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		if len(repo.GetLastEventsCalls()) == 1 {
			return nil, errors.New("get-last-events-error")
		}
		return nil, nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.Run(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()

	calls := repo.GetLastEventsCalls()
	assert.Equal(t, 2, len(calls))
	assert.Equal(t, ctx, calls[0].Ctx)
	assert.Equal(t, uint64(1000), calls[0].Limit)
}

func TestRunner__GetLastEvents_Error__With_Publisher(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
	publisher := &PublisherMock{}

	r := New(repo, WithErrorSleepDuration(30*time.Millisecond),
		WithPublisher(100, publisher),
	)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	repo.GetLastSequenceFunc = func(ctx context.Context, id PublisherID) (uint64, error) {
		return 0, nil
	}

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		if len(repo.GetLastEventsCalls()) == 1 {
			return nil, errors.New("get-last-events-error")
		}
		return nil, nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.Run(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()

	assert.Equal(t, 2, len(repo.GetLastEventsCalls()))
	calls := repo.GetLastSequenceCalls()
	assert.Equal(t, 1, len(calls))
}

func TestRunner__GetLastSequence_Error(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
	publisher := &PublisherMock{}

	r := New(repo, WithErrorSleepDuration(30*time.Millisecond),
		WithPublisher(100, publisher),
	)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	repo.GetLastSequenceFunc = func(ctx context.Context, id PublisherID) (uint64, error) {
		if len(repo.GetLastSequenceCalls()) == 1 {
			return 0, errors.New("get-last-seq-error")
		}
		return 0, nil
	}

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return nil, nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.Run(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()

	assert.Equal(t, 2, len(repo.GetLastEventsCalls()))
	calls := repo.GetLastSequenceCalls()
	assert.Equal(t, 2, len(calls))
}

func TestRunner__GetLastEvents_Error_Context_Cancelled(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
	r := New(repo, WithErrorSleepDuration(30*time.Millisecond))

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		if len(repo.GetLastEventsCalls()) == 1 {
			return nil, errors.New("get-last-events-error")
		}
		time.Sleep(30 * time.Millisecond)
		return nil, nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.Run(ctx)
	}()

	time.Sleep(45 * time.Millisecond)
	cancel()
	wg.Wait()

	calls := repo.GetLastEventsCalls()
	assert.Equal(t, 2, len(calls))
	assert.Equal(t, ctx, calls[0].Ctx)
	assert.Equal(t, uint64(1000), calls[0].Limit)
}

func TestRunner__Processor_Timer_Expired(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
	r := New(repo, WithRetryDuration(20*time.Millisecond))

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return nil, nil
	}
	repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return nil, nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.Run(ctx)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()
	wg.Wait()

	assert.Equal(t, 1, len(repo.GetLastEventsCalls()))
	assert.Equal(t, 1, len(repo.GetUnprocessedEventsCalls()))
}

func TestRunner_Processor_Run_Error(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
	publisher := &PublisherMock{}

	r := New(repo, WithRetryDuration(20*time.Millisecond),
		WithPublisher(100, publisher),
	)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return nil, nil
	}
	repo.GetLastSequenceFunc = func(ctx context.Context, id PublisherID) (uint64, error) {
		return 0, nil
	}
	repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return []Event{
			{ID: 100, Sequence: 10},
			{ID: 99, Sequence: 18},
		}, nil
	}
	repo.UpdateSequencesFunc = func(ctx context.Context, events []Event) error {
		return errors.New("update-seq-error")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.Run(ctx)
	}()

	time.Sleep(40 * time.Millisecond)
	cancel()
	wg.Wait()

	assert.Equal(t, 2, len(repo.GetLastEventsCalls()))
}

func TestRunner_Processor_Signal_Observer_OK(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
	publisher := &PublisherMock{}

	r := New(repo, WithPublisher(100, publisher))

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return nil, nil
	}
	repo.GetLastSequenceFunc = func(ctx context.Context, id PublisherID) (uint64, error) {
		return 0, nil
	}
	repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return []Event{
			{ID: 100},
			{ID: 99},
			{ID: 101},
			{ID: 120},
			{ID: 130},
		}, nil
	}
	repo.UpdateSequencesFunc = func(ctx context.Context, events []Event) error {
		return nil
	}
	publisher.PublishFunc = func(ctx context.Context, events []Event) error {
		return nil
	}
	repo.SaveLastSequenceFunc = func(ctx context.Context, id PublisherID, seq uint64) error {
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.Run(ctx)
	}()

	r.Signal()

	obs := r.NewObserver(1, 3)
	events := obs.GetNextEvents(ctx, nil)
	assert.Equal(t, []Event{
		{ID: 100, Sequence: 1},
		{ID: 99, Sequence: 2},
		{ID: 101, Sequence: 3},
	}, events)

	time.Sleep(40 * time.Millisecond)
	cancel()
	wg.Wait()

	assert.Equal(t, 1, len(repo.GetLastEventsCalls()))
	assert.Equal(t, 1, len(repo.GetUnprocessedEventsCalls()))

	publishCalls := publisher.PublishCalls()
	assert.Equal(t, 1, len(publishCalls))
	assert.Equal(t, []Event{
		{ID: 100, Sequence: 1},
		{ID: 99, Sequence: 2},
		{ID: 101, Sequence: 3},
		{ID: 120, Sequence: 4},
		{ID: 130, Sequence: 5},
	}, publishCalls[0].Events)

	saveCalls := repo.SaveLastSequenceCalls()
	assert.Equal(t, 1, len(saveCalls))
	assert.Equal(t, PublisherID(100), saveCalls[0].ID)
	assert.Equal(t, uint64(5), saveCalls[0].Seq)
}

func TestRunner_Processor_Signal_WaitPublishers_OK(t *testing.T) {
	t.Parallel()

	repo := &RepositoryMock{}
	publisher := &PublisherMock{}

	r := New(repo, WithPublisher(100, publisher))

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	repo.GetLastEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return nil, nil
	}
	repo.GetLastSequenceFunc = func(ctx context.Context, id PublisherID) (uint64, error) {
		return 0, nil
	}
	repo.GetUnprocessedEventsFunc = func(ctx context.Context, limit uint64) ([]Event, error) {
		return []Event{
			{ID: 100},
			{ID: 99},
			{ID: 101},
			{ID: 120},
			{ID: 130},
		}, nil
	}
	repo.UpdateSequencesFunc = func(ctx context.Context, events []Event) error {
		return nil
	}
	publisher.PublishFunc = func(ctx context.Context, events []Event) error {
		return nil
	}
	repo.SaveLastSequenceFunc = func(ctx context.Context, id PublisherID, seq uint64) error {
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.Run(ctx)
	}()

	r.Signal()

	seq := r.WaitPublishers(ctx, 120, 100)
	assert.Equal(t, uint64(4), seq)

	time.Sleep(40 * time.Millisecond)
	cancel()
	wg.Wait()

	assert.Equal(t, 1, len(repo.GetLastEventsCalls()))
	assert.Equal(t, 1, len(repo.GetUnprocessedEventsCalls()))

	publishCalls := publisher.PublishCalls()
	assert.Equal(t, 1, len(publishCalls))
	assert.Equal(t, []Event{
		{ID: 100, Sequence: 1},
		{ID: 99, Sequence: 2},
		{ID: 101, Sequence: 3},
		{ID: 120, Sequence: 4},
		{ID: 130, Sequence: 5},
	}, publishCalls[0].Events)

	saveCalls := repo.SaveLastSequenceCalls()
	assert.Equal(t, 1, len(saveCalls))
	assert.Equal(t, PublisherID(100), saveCalls[0].ID)
	assert.Equal(t, uint64(5), saveCalls[0].Seq)
}
