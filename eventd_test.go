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
