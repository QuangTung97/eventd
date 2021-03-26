package eventd

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	// DefaultGetEventsLimit ...
	DefaultGetEventsLimit uint64 = 1000
)

// Event ...
type Event struct {
	ID        uint64
	Sequence  uint64
	Data      string
	CreatedAt time.Time
}

//go:generate mockgen -destination=repository_mocks.go -self_package=github.com/QuangTung97/eventd -package=eventd . Repository

// Repository ...
type Repository interface {
	GetLastEvents(ctx context.Context, limit uint64) ([]Event, error)
	GetUnprocessedEvents(ctx context.Context, limit uint64) ([]Event, error)

	UpdateSequences(ctx context.Context, events []Event) error

	GetLastSequence(ctx context.Context, id PublisherID) (uint64, error)
	SaveLastSequence(ctx context.Context, id PublisherID, seq uint64) error
}

// PublisherID ...
type PublisherID uint32

//go:generate mockgen -destination=publisher_mocks.go -self_package=github.com/QuangTung97/eventd -package=eventd . Publisher

// Publisher ...
type Publisher interface {
	Publish(ctx context.Context, events []Event) error
}

// Run ...
//gocyclo:ignore
func Run(ctx context.Context, repo Repository, signals <-chan struct{}, opts ...Option) {
OuterLoop:
	for {
		options := defaultRunnerOpts
		for _, o := range opts {
			o(&options)
		}

		p := newProcessor(repo, options)
		err := p.init(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			fmt.Println(err)
			// TODO: sleep context
			continue OuterLoop
		}

		// TODO not default get events limit
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
				fmt.Println(err)
				// TODO: sleep context
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
					fmt.Println(err)
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
						fmt.Println(err)
						// TODO sleep with context
						time.Sleep(10 * time.Second)
						return
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
