package eventd

type runnerOpts struct {
	getLastEventsLimit uint64
	storedEventSize    uint64
}

// Option ...
type Option func(opts *runnerOpts)

func defaultRunnerOpts() runnerOpts {
	return runnerOpts{
		getLastEventsLimit: DefaultGetLastEventsLimit,
		storedEventSize:    DefaultGetLastEventsLimit,
	}
}

// WithGetLastEventsLimit ...
func WithGetLastEventsLimit(limit uint64) Option {
	return func(opts *runnerOpts) {
		opts.getLastEventsLimit = limit
	}
}
