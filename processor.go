package eventd

type processor struct {
	repo               Repository
	getLastEventsLimit uint64
	storedEvents       []Event
	lastSequence       uint64
	firstSequence      uint64
}

func newProcessor(repo Repository, opts runnerOpts) *processor {
	return &processor{
		repo:               repo,
		getLastEventsLimit: opts.getLastEventsLimit,
		storedEvents:       make([]Event, opts.storedEventSize),
		lastSequence:       0,
		firstSequence:      0,
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

	size := uint64(len(p.storedEvents))

	p.firstSequence = events[0].Sequence
	p.lastSequence = events[len(events)-1].Sequence
	if p.firstSequence+size < p.lastSequence+1 {
		p.firstSequence = p.lastSequence + 1 - size
	}

	for _, e := range events {
		p.storedEvents[e.Sequence%size] = e
	}
	return nil
}

func (p *processor) currentEvents() []Event {
	var result []Event
	if p.lastSequence == 0 {
		return nil
	}

	size := uint64(len(p.storedEvents))
	for seq := p.firstSequence; seq <= p.lastSequence; seq++ {
		result = append(result, p.storedEvents[seq%size])
	}
	return result
}

func (p *processor) signal() {
}
