package eventd

import (
	"context"
	"errors"
	"go.uber.org/zap"
	"sync"
	"time"
)

const (
	// DefaultGetEventsLimit ...
	DefaultGetEventsLimit uint64 = 1000
)

// ErrEventsNotFound when GetEventsFrom returns empty, indicating some events are missing
var ErrEventsNotFound = errors.New("events not found")

// Event ...
type Event struct {
	ID        uint64
	Sequence  uint64
	Data      string
	CreatedAt time.Time
}

//go:generate moq -out generated_moq_test.go . Repository Publisher

// Repository ...
type Repository interface {
	GetLastEvents(ctx context.Context, limit uint64) ([]Event, error)
	GetUnprocessedEvents(ctx context.Context, limit uint64) ([]Event, error)
	GetEventsFrom(ctx context.Context, from uint64, limit uint64) ([]Event, error)

	UpdateSequences(ctx context.Context, events []Event) error

	GetLastSequence(ctx context.Context, id PublisherID) (uint64, error)
	SaveLastSequence(ctx context.Context, id PublisherID, seq uint64) error
}

// PublisherID ...
type PublisherID uint32

// Publisher ...
type Publisher interface {
	Publish(ctx context.Context, events []Event) error
}

func sleepContext(ctx context.Context, duration time.Duration) {
	select {
	case <-time.After(duration):
		return
	case <-ctx.Done():
		return
	}
}

// Run ...
//gocyclo:ignore
func Run(ctx context.Context, repo Repository, signals <-chan struct{}, opts ...Option) {
OuterLoop:
	for {
		options := computeRunnerOpts(opts...)
		logger := options.logger

		p := newProcessor(repo, options)
		err := p.init(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			logger.Error("eventd.processor.init", zap.Error(err))
			sleepContext(ctx, options.errorSleepDuration)
			continue OuterLoop
		}

		fetchRequestChan := make(chan fetchRequest, DefaultGetEventsLimit)
		publishers := map[PublisherID]*publisherRunner{}

		for id, publisherConf := range options.publishers {
			conf := publisherConf

			publisher := newPublisherRunner(id, repo,
				conf.publisher, fetchRequestChan, conf.options)
			err := publisher.init(ctx)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				logger.Error("eventd.publisher.init", zap.Error(err))
				sleepContext(ctx, options.errorSleepDuration)
				continue OuterLoop
			}
			publishers[id] = publisher
		}

		runningCtx, cancel := context.WithCancel(ctx)
		var wg sync.WaitGroup

		wg.Add(1 + len(publishers))
		go func() {
			defer wg.Done()

			for {
				err := p.run(runningCtx, signals, fetchRequestChan)
				if runningCtx.Err() != nil {
					return
				}
				if err != nil {
					logger.Error("eventd.processor.run", zap.Error(err))
					cancel()
					return
				}
			}
		}()

		for id, publisherConf := range options.publishers {
			publisher := publishers[id]
			waitRequestChan := publisherConf.waitRequestChan

			go func() {
				defer wg.Done()

				for {
					if !publisher.isFetching() {
						publisher.fetch()
					}

					err := publisher.run(runningCtx, waitRequestChan)
					if runningCtx.Err() != nil {
						return
					}
					if err != nil {
						logger.Error("eventd.publisher.run", zap.Error(err))
						sleepContext(runningCtx, options.errorSleepDuration)
						continue
					}
				}
			}()
		}

		<-runningCtx.Done()
		wg.Wait()

		if ctx.Err() != nil {
			return
		}
	}
}
