package eventd

type runnerOpts struct {
	getEventsLimit  uint64
	storedEventSize uint64
}

// Option ...
type Option func(opts *runnerOpts)

func defaultRunnerOpts() runnerOpts {
	return runnerOpts{
		getEventsLimit:  DefaultGetEventsLimit,
		storedEventSize: DefaultGetEventsLimit,
	}
}
