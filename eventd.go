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

// Runner ...
type Runner struct {
	options runnerOpts
	repo    Repository

	signalChan chan struct{}
	fetchChan  chan fetchRequest
	proc       *processor
	publishers map[PublisherID]*publisherRunner
}

// New ...
func New(repo Repository, options ...Option) *Runner {
	opts := computeRunnerOpts(options...)

	proc := newProcessor(repo, opts)

	signalChan := make(chan struct{}, DefaultGetEventsLimit)
	// TODO fetchRequestChan cap
	fetchChan := make(chan fetchRequest, DefaultGetEventsLimit)
	waitRequestChan := make(chan waitRequest, DefaultGetEventsLimit)

	publishers := map[PublisherID]*publisherRunner{}
	for id, publisherConf := range opts.publishers {
		conf := publisherConf

		publisher := newPublisherRunner(id, repo,
			conf.publisher, fetchChan, waitRequestChan, conf.options)
		publishers[id] = publisher
	}

	return &Runner{
		options: opts,
		repo:    repo,

		signalChan: signalChan,
		fetchChan:  fetchChan,
		proc:       proc,
		publishers: publishers,
	}
}

// Run ...
//gocyclo:ignore
func (r *Runner) Run(ctx context.Context) {
	logger := r.options.logger

OuterLoop:
	for {
		err := r.proc.init(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			logger.Error("eventd.processor.init", zap.Error(err))
			sleepContext(ctx, r.options.errorSleepDuration)
			continue OuterLoop
		}

		for _, publisher := range r.publishers {
			err := publisher.init(ctx)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				logger.Error("eventd.publisher.init", zap.Error(err))
				sleepContext(ctx, r.options.errorSleepDuration)
				continue OuterLoop
			}
		}

		runningCtx, cancel := context.WithCancel(ctx)
		var wg sync.WaitGroup

		wg.Add(1 + len(r.publishers))
		go func() {
			defer wg.Done()

			for {
				err := r.proc.run(runningCtx, r.signalChan, r.fetchChan)
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

		for _, p := range r.publishers {
			publisher := p

			go func() {
				defer wg.Done()

				for {
					if !publisher.isFetching() {
						publisher.fetch()
					}

					err := publisher.run(runningCtx)
					if runningCtx.Err() != nil {
						return
					}
					if err != nil {
						logger.Error("eventd.publisher.run", zap.Error(err))
						sleepContext(runningCtx, r.options.errorSleepDuration)
						continue
					}
				}
			}()
		}

		wg.Wait()

		if ctx.Err() != nil {
			return
		}
	}
}

// Signal ...
func (r *Runner) Signal() {
	r.signalChan <- struct{}{}
}

// NewObserver ...
func (r *Runner) NewObserver(fromSequence uint64, limit uint64, options ...ObserverOption) *Observer {
	return newObserver(r.repo, r.fetchChan, fromSequence, limit, options...)
}
