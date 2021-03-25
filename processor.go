package eventd

import "context"

type processorCheckStatus uint32

const (
	processorCheckStatusOK       processorCheckStatus = 0
	processorCheckStatusNeedWait processorCheckStatus = 1
	processorCheckStatusTooSmall processorCheckStatus = 2
)

type fetchResponse struct {
	existed bool
	result  []Event
}

type fetchRequest struct {
	from         uint64
	limit        uint64
	result       []Event
	responseChan chan<- fetchResponse
}

type processor struct {
	repo                Repository
	getLastEventsLimit  uint64
	storedEvents        []Event
	lastSequence        uint64
	beforeFirstSequence uint64
	waitList            []fetchRequest
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

func (p *processor) init(ctx context.Context) error {
	events, err := p.repo.GetLastEvents(ctx, p.getLastEventsLimit)
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

func (p *processor) signal(ctx context.Context) error {
	events, err := p.repo.GetUnprocessedEvents(ctx, p.getLastEventsLimit)
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

	err = p.repo.UpdateSequences(ctx, events)
	if err != nil {
		return err
	}

	p.storeEvents(events)

	return nil
}

func (p *processor) checkFromSequence(seq uint64) processorCheckStatus {
	if seq > p.lastSequence {
		return processorCheckStatusNeedWait
	}
	if seq <= p.beforeFirstSequence {
		return processorCheckStatusTooSmall
	}
	return processorCheckStatusOK
}

func (p *processor) getEventsFrom(from uint64, result []Event, limit uint64) []Event {
	size := uint64(len(p.storedEvents))
	last := from + limit - 1
	if last > p.lastSequence {
		last = p.lastSequence
	}
	for seq := from; seq <= last; seq++ {
		result = append(result, p.storedEvents[seq%size])
	}
	return result
}

func (p *processor) getLastSequence() uint64 {
	return p.lastSequence
}

func (p *processor) handleSignals(ctx context.Context, signals <-chan struct{}) error {
DrainLoop:
	for {
		select {
		case <-signals:
			continue
		default:
			break DrainLoop
		}
	}

	lastSeq := p.lastSequence
	err := p.signal(ctx)
	if p.lastSequence > lastSeq {
		for i, waitRequest := range p.waitList {
			waitRequest.responseChan <- fetchResponse{
				existed: true,
				result:  p.getEventsFrom(lastSeq+1, waitRequest.result, waitRequest.limit),
			}
			p.waitList[i].result = nil
			p.waitList[i].responseChan = nil
		}
		p.waitList = p.waitList[:0]
	}
	return err
}

func (p *processor) run(
	ctx context.Context, signals <-chan struct{},
	fetchChan <-chan fetchRequest,
) error {
	select {
	case <-signals:
		return p.handleSignals(ctx, signals)

	case request := <-fetchChan:
		switch p.checkFromSequence(request.from) {
		case processorCheckStatusOK:
			request.responseChan <- fetchResponse{
				existed: true,
				result:  p.getEventsFrom(request.from, request.result, request.limit),
			}

		case processorCheckStatusTooSmall:
			request.responseChan <- fetchResponse{
				existed: false,
				result:  request.result,
			}

		case processorCheckStatusNeedWait:
			p.waitList = append(p.waitList, request)
		}
		return nil

	case <-ctx.Done():
		return nil
	}
}
