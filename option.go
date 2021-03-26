package eventd

type runnerOpts struct {
	getEventsLimit  uint64
	storedEventSize uint64
	waitListLimit   uint64
}

// Option ...
type Option func(opts *runnerOpts)

func defaultRunnerOpts() runnerOpts {
	return runnerOpts{
		getEventsLimit:  DefaultGetEventsLimit,
		storedEventSize: DefaultGetEventsLimit,
	}
}

type publisherOpts struct {
	processedListLimit uint64
	waitListLimit      uint64
	publishLimit       uint64
}
