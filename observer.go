package eventd

import (
	"context"
	"time"
)

// Observer ...
type Observer struct {
	repo          Repository
	errorDuration time.Duration

	fetchChan    chan<- fetchRequest
	fromSequence uint64
	limit        uint64
	cacheResult  []Event
	responseChan chan fetchResponse
	logger       func(err error)
}

func newObserver(
	repo Repository, fetchChan chan<- fetchRequest,
	fromSequence uint64, limit uint64,
	options ...ObserverOption,
) *Observer {
	opts := observerOpts{
		logger:             func(err error) {},
		errorSleepDuration: 30 * time.Second,
	}
	for _, o := range options {
		o(&opts)
	}

	return &Observer{
		repo:          repo,
		errorDuration: opts.errorSleepDuration,

		fetchChan:    fetchChan,
		fromSequence: fromSequence,
		limit:        limit,
		cacheResult:  make([]Event, 0, limit),
		responseChan: make(chan fetchResponse, 1),
		logger:       opts.logger,
	}
}

func (o *Observer) beforeGetEvents() {
	o.fetchChan <- fetchRequest{
		from:         o.fromSequence,
		limit:        o.limit,
		result:       o.cacheResult,
		responseChan: o.responseChan,
	}
}

func (o *Observer) waitForEvents(ctx context.Context, result []Event) ([]Event, error) {
	select {
	case resp := <-o.responseChan:
		if !resp.existed {
			events, err := o.repo.GetEventsFrom(ctx, o.fromSequence, o.limit)
			if err != nil {
				o.logger(err)
				return result, err
			}
			resp.result = events
		}
		o.fromSequence = resp.result[len(resp.result)-1].Sequence + 1
		result = append(result, resp.result...)
		return result, nil

	case <-ctx.Done():
		return result, nil
	}
}

// GetNextEvents ...
func (o *Observer) GetNextEvents(ctx context.Context, result []Event) []Event {
	for {
		o.beforeGetEvents()

		var err error
		result, err = o.waitForEvents(ctx, result)
		if ctx.Err() != nil {
			return result
		}
		if err != nil {
			sleepContext(ctx, o.errorDuration)
			continue
		}
		return result
	}
}
