package eventd

type processor struct {
	repo                Repository
	getLastEventsLimit  uint64
	storedEvents        []Event
	lastSequence        uint64
	beforeFirstSequence uint64
}

func newProcessor(repo Repository, opts runnerOpts) *processor {
	return &processor{
		repo:                repo,
		getLastEventsLimit:  opts.getEventsLimit,
		storedEvents:        make([]Event, opts.storedEventSize),
		lastSequence:        0,
		beforeFirstSequence: 0,
	}
}

func (p *processor) init() error {
	events, err := p.repo.GetLastEvents(p.getLastEventsLimit)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}

	p.beforeFirstSequence = events[0].Sequence - 1
	p.storeEvents(events)
	return nil
}

func (p *processor) currentEvents() []Event {
	var result []Event

	size := uint64(len(p.storedEvents))
	for seq := p.beforeFirstSequence + 1; seq <= p.lastSequence; seq++ {
		result = append(result, p.storedEvents[seq%size])
	}
	return result
}

func (p *processor) storeEvents(events []Event) {
	size := uint64(len(p.storedEvents))
	p.lastSequence = events[len(events)-1].Sequence
	if p.beforeFirstSequence+size < p.lastSequence {
		p.beforeFirstSequence = p.lastSequence - size
	}
	for _, e := range events {
		p.storedEvents[e.Sequence%size] = e
	}
}

func (p *processor) signal() error {
	events, err := p.repo.GetUnprocessedEvents(p.getLastEventsLimit)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}

	lastSequence := p.lastSequence
	for i := range events {
		lastSequence++
		events[i].Sequence = lastSequence
	}

	err = p.repo.UpdateSequences(events)
	if err != nil {
		return err
	}

	p.storeEvents(events)

	return nil
}
