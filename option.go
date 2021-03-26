package eventd

import (
	"go.uber.org/zap"
	"time"
)

// PUBLISHER OPTIONS

type publisherOpts struct {
	processedListLimit uint64
	waitListLimit      uint64
	publishLimit       uint64
}

type registeredPublisher struct {
	options         publisherOpts
	publisher       Publisher
	waitRequestChan <-chan waitRequest
}

// PublisherOption ...
type PublisherOption func(opts *publisherOpts)

var defaultPublisherOptions = publisherOpts{
	processedListLimit: DefaultGetEventsLimit,
	waitListLimit:      DefaultGetEventsLimit,
	publishLimit:       DefaultGetEventsLimit,
}

// RUNNER OPTIONS

type runnerOpts struct {
	getEventsLimit     uint64
	storedEventSize    uint64
	errorSleepDuration time.Duration
	publishers         map[PublisherID]registeredPublisher
	logger             *zap.Logger
}

// Option ...
type Option func(opts *runnerOpts)

var defaultRunnerOpts = runnerOpts{
	getEventsLimit:     DefaultGetEventsLimit,
	storedEventSize:    DefaultGetEventsLimit,
	errorSleepDuration: 30 * time.Second,
	publishers:         map[PublisherID]registeredPublisher{},
	logger:             zap.NewNop(),
}

// WithPublisher ...
func WithPublisher(
	id PublisherID, publisher Publisher,
	waitRequestChan <-chan waitRequest, options ...PublisherOption,
) Option {
	return func(opts *runnerOpts) {
		publisherOptions := defaultPublisherOptions
		for _, o := range options {
			o(&publisherOptions)
		}

		opts.publishers[id] = registeredPublisher{
			publisher:       publisher,
			options:         publisherOptions,
			waitRequestChan: waitRequestChan,
		}
	}
}

// WithLogger ...
func WithLogger(logger *zap.Logger) Option {
	return func(opts *runnerOpts) {
		opts.logger = logger
	}
}
