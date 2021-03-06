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
	options   publisherOpts
	publisher Publisher
}

// PublisherOption ...
type PublisherOption func(opts *publisherOpts)

func defaultPublisherOptions() publisherOpts {
	return publisherOpts{
		processedListLimit: DefaultGetEventsLimit,
		waitListLimit:      DefaultGetEventsLimit,
		publishLimit:       DefaultGetEventsLimit,
	}
}

func computePublisherOpts(options ...PublisherOption) publisherOpts {
	result := defaultPublisherOptions()
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
	retryDuration      time.Duration
	storedEventSize    uint64
	errorSleepDuration time.Duration
	publishers         map[PublisherID]registeredPublisher
	logger             *zap.Logger
}

// Option ...
type Option func(opts *runnerOpts)

func defaultRunnerOpts() runnerOpts {
	return runnerOpts{
		getEventsLimit:     DefaultGetEventsLimit,
		retryDuration:      60 * time.Second,
		storedEventSize:    DefaultGetEventsLimit,
		errorSleepDuration: 30 * time.Second,
		publishers:         map[PublisherID]registeredPublisher{},
		logger:             zap.NewNop(),
	}
}

func computeRunnerOpts(options ...Option) runnerOpts {
	result := defaultRunnerOpts()
	for _, o := range options {
		o(&result)
	}
	return result
}

// WithPublisher ...
func WithPublisher(
	id PublisherID, publisher Publisher,
	options ...PublisherOption,
) Option {
	return func(opts *runnerOpts) {
		opts.publishers[id] = registeredPublisher{
			publisher: publisher,
			options:   computePublisherOpts(options...),
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

// WithRetryDuration ...
func WithRetryDuration(d time.Duration) Option {
	return func(opts *runnerOpts) {
		opts.retryDuration = d
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

// OBSERVER OPTIONS
type observerOpts struct {
	logger             func(err error)
	errorSleepDuration time.Duration
}

// ObserverOption ...
type ObserverOption func(opts *observerOpts)

// WithObserverLogger ...
func WithObserverLogger(logger func(err error)) ObserverOption {
	return func(opts *observerOpts) {
		opts.logger = logger
	}
}
