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

func computePublisherOpts(options ...PublisherOption) publisherOpts {
	result := defaultPublisherOptions
	for _, o := range options {
		o(&result)
	}
	return result
}

// WithProcessedListLimit ...
func WithProcessedListLimit(limit uint64) PublisherOption {
	return func(opts *publisherOpts) {
		opts.processedListLimit = limit
	}
}

// WithWaitListLimit ...
func WithWaitListLimit(limit uint64) PublisherOption {
	return func(opts *publisherOpts) {
		opts.waitListLimit = limit
	}
}

// WithPublishLimit ...
func WithPublishLimit(limit uint64) PublisherOption {
	return func(opts *publisherOpts) {
		opts.publishLimit = limit
	}
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

func computeRunnerOpts(options ...Option) runnerOpts {
	result := defaultRunnerOpts
	for _, o := range options {
		o(&result)
	}
	return result
}

// WithPublisher ...
func WithPublisher(
	id PublisherID, publisher Publisher,
	waitRequestChan <-chan waitRequest, options ...PublisherOption,
) Option {
	return func(opts *runnerOpts) {
		opts.publishers[id] = registeredPublisher{
			publisher:       publisher,
			options:         computePublisherOpts(options...),
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

// WithGetEventsLimit ...
func WithGetEventsLimit(limit uint64) Option {
	return func(opts *runnerOpts) {
		opts.getEventsLimit = limit
	}
}

// WithStoredEventsSize ...
func WithStoredEventsSize(size uint64) Option {
	return func(opts *runnerOpts) {
		opts.storedEventSize = size
	}
}

// WithErrorSleepDuration ...
func WithErrorSleepDuration(d time.Duration) Option {
	return func(opts *runnerOpts) {
		opts.errorSleepDuration = d
	}
}
