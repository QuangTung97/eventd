package eventd

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

type runnerOpts struct {
	getEventsLimit  uint64
	storedEventSize uint64
	publishers      map[PublisherID]registeredPublisher
}

// Option ...
type Option func(opts *runnerOpts)

var defaultPublisherOptions = publisherOpts{
	processedListLimit: DefaultGetEventsLimit,
	waitListLimit:      DefaultGetEventsLimit,
	publishLimit:       DefaultGetEventsLimit,
}

var defaultRunnerOpts = runnerOpts{
	getEventsLimit:  DefaultGetEventsLimit,
	storedEventSize: DefaultGetEventsLimit,
}

// WithPublisher ...
func WithPublisher(id PublisherID, publisher Publisher, options ...PublisherOption) Option {
	return func(opts *runnerOpts) {
		publisherOptions := defaultPublisherOptions
		for _, o := range options {
			o(&publisherOptions)
		}

		opts.publishers[id] = registeredPublisher{
			options: publisherOptions,
		}
	}
}
