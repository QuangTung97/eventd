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
