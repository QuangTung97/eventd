package eventd

import (
	"context"
	"errors"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestProcessor_Init__GetLastEvents_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	ctx := context.Background()

	repo.EXPECT().GetLastEvents(ctx, DefaultGetEventsLimit).
		Return(nil, errors.New("get-last-events-error"))

	p := newProcessor(repo, defaultRunnerOpts)
	err := p.init(ctx)
	assert.Equal(t, errors.New("get-last-events-error"), err)
}

func TestProcessor_Init__Without_Last_Events(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	ctx := context.Background()

	repo.EXPECT().GetLastEvents(ctx, DefaultGetEventsLimit).Return(nil, nil)

	p := newProcessor(repo, defaultRunnerOpts)
	err := p.init(ctx)

	assert.Equal(t, nil, err)
	assert.Equal(t, []Event(nil), p.currentEvents())
}

func TestProcessor_Init__With_Last_Events(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	ctx := context.Background()

	events := []Event{
		{ID: 10, Sequence: 5},
		{ID: 9, Sequence: 6},
		{ID: 11, Sequence: 7},
	}
	repo.EXPECT().GetLastEvents(ctx, DefaultGetEventsLimit).
		Return(events, nil)

	p := newProcessor(repo, defaultRunnerOpts)
	err := p.init(ctx)

	assert.Equal(t, nil, err)
	assert.Equal(t, events, p.currentEvents())
}

func TestProcessor_Init__With_Last_3_Events__Limit_3(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	ctx := context.Background()

	events := []Event{
		{ID: 10, Sequence: 5},
		{ID: 9, Sequence: 6},
		{ID: 11, Sequence: 7},
	}
	repo.EXPECT().GetLastEvents(ctx, uint64(4)).
		Return(events, nil)

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 3,
	})
	err := p.init(ctx)

	assert.Equal(t, nil, err)
	assert.Equal(t, events, p.currentEvents())
}

func TestProcessor_Init__With_Last_3_Events__Limit_2(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	ctx := context.Background()

	events := []Event{
		{ID: 10, Sequence: 5},
		{ID: 9, Sequence: 6},
		{ID: 11, Sequence: 7},
	}
	repo.EXPECT().GetLastEvents(ctx, uint64(4)).
		Return(events, nil)

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 2,
	})
	err := p.init(ctx)

	assert.Equal(t, nil, err)
	assert.Equal(t, events[1:], p.currentEvents())
}

func TestProcessor_Signal__GetUnprocessed_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	ctx := context.Background()
	repo.EXPECT().GetLastEvents(ctx, gomock.Any()).Return(nil, nil)
	repo.EXPECT().GetUnprocessedEvents(ctx, uint64(4)).
		Return(nil, errors.New("get-unprocessed-error"))

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 3,
	})
	_ = p.init(ctx)

	err := p.signal(ctx)
	assert.Equal(t, errors.New("get-unprocessed-error"), err)
}

func TestProcessor_Signal__Empty_Unprocessed__Without_Stored(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	ctx := context.Background()
	repo.EXPECT().GetLastEvents(ctx, gomock.Any()).Return(nil, nil)
	repo.EXPECT().GetUnprocessedEvents(ctx, uint64(4)).
		Return(nil, nil)

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 3,
	})
	_ = p.init(ctx)

	err := p.signal(ctx)
	assert.Equal(t, nil, err)
	assert.Equal(t, []Event(nil), p.currentEvents())
}

func TestProcessor_Signal__Empty_Unprocessed__With_Stored(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	ctx := context.Background()
	events := []Event{
		{ID: 10, Sequence: 5},
		{ID: 9, Sequence: 6},
		{ID: 11, Sequence: 7},
	}
	repo.EXPECT().GetLastEvents(ctx, gomock.Any()).Return(events, nil)
	repo.EXPECT().GetUnprocessedEvents(ctx, uint64(4)).
		Return(nil, nil)

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 3,
	})
	_ = p.init(ctx)

	err := p.signal(ctx)
	assert.Equal(t, nil, err)
	assert.Equal(t, events, p.currentEvents())
}

func TestProcessor_Signal__4_Unprocessed__Update_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	ctx := context.Background()
	repo.EXPECT().GetLastEvents(ctx, gomock.Any()).Return(nil, nil)
	repo.EXPECT().GetUnprocessedEvents(ctx, gomock.Any()).
		Return([]Event{
			{ID: 10},
			{ID: 12},
			{ID: 7},
			{ID: 15},
		}, nil)

	repo.EXPECT().UpdateSequences(ctx, []Event{
		{ID: 10, Sequence: 1},
		{ID: 12, Sequence: 2},
		{ID: 7, Sequence: 3},
		{ID: 15, Sequence: 4},
	}).Return(errors.New("update-sequences-error"))

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 3,
	})
	_ = p.init(ctx)

	err := p.signal(ctx)
	assert.Equal(t, errors.New("update-sequences-error"), err)
	assert.Equal(t, []Event(nil), p.currentEvents())
}

func TestProcessor_Signal__4_Unprocessed__Update_OK(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	ctx := context.Background()
	repo.EXPECT().GetLastEvents(ctx, gomock.Any()).Return(nil, nil)
	repo.EXPECT().GetUnprocessedEvents(ctx, gomock.Any()).
		Return([]Event{
			{ID: 10},
			{ID: 12},
			{ID: 7},
			{ID: 15},
		}, nil)

	repo.EXPECT().UpdateSequences(ctx, []Event{
		{ID: 10, Sequence: 1},
		{ID: 12, Sequence: 2},
		{ID: 7, Sequence: 3},
		{ID: 15, Sequence: 4},
	}).Return(nil)

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 3,
	})
	_ = p.init(ctx)

	err := p.signal(ctx)
	assert.Equal(t, nil, err)
	assert.Equal(t, []Event{
		{ID: 12, Sequence: 2},
		{ID: 7, Sequence: 3},
		{ID: 15, Sequence: 4},
	}, p.currentEvents())
}

func TestProcessor_Signal__With_Stored__OK(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	ctx := context.Background()
	events := []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}
	repo.EXPECT().GetLastEvents(ctx, gomock.Any()).Return(events, nil)
	repo.EXPECT().GetUnprocessedEvents(ctx, gomock.Any()).
		Return([]Event{
			{ID: 10},
			{ID: 12},
			{ID: 7},
			{ID: 15},
		}, nil)

	repo.EXPECT().UpdateSequences(ctx, []Event{
		{ID: 10, Sequence: 8},
		{ID: 12, Sequence: 9},
		{ID: 7, Sequence: 10},
		{ID: 15, Sequence: 11},
	}).Return(nil)

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 5,
	})
	_ = p.init(ctx)

	err := p.signal(ctx)
	assert.Equal(t, nil, err)
	assert.Equal(t, []Event{
		{ID: 5, Sequence: 7},
		{ID: 10, Sequence: 8},
		{ID: 12, Sequence: 9},
		{ID: 7, Sequence: 10},
		{ID: 15, Sequence: 11},
	}, p.currentEvents())
	assert.Equal(t, uint64(11), p.getLastSequence())
}

func TestProcessor_Check_From_Sequence(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	ctx := context.Background()
	events := []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}
	repo.EXPECT().GetLastEvents(ctx, gomock.Any()).Return(events, nil)

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 5,
	})
	_ = p.init(ctx)

	table := []struct {
		name   string
		from   uint64
		status processorCheckStatus
	}{
		{
			name:   "check-ok",
			from:   7,
			status: processorCheckStatusOK,
		},
		{
			name:   "check-ok",
			from:   5,
			status: processorCheckStatusOK,
		},
		{
			name:   "need-wait",
			from:   8,
			status: processorCheckStatusNeedWait,
		},
		{
			name:   "need-wait",
			from:   9,
			status: processorCheckStatusNeedWait,
		},
		{
			name:   "too-small",
			from:   4,
			status: processorCheckStatusTooSmall,
		},
	}
	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			status := p.checkFromSequence(e.from)
			assert.Equal(t, e.status, status)
		})
	}
}

func TestProcessor_Get_Events_From(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	ctx := context.Background()
	events := []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}
	repo.EXPECT().GetLastEvents(ctx, gomock.Any()).Return(events, nil)

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 5,
	})
	_ = p.init(ctx)

	assert.Equal(t, uint64(7), p.getLastSequence())

	result := p.getEventsFrom(5, nil, 1)
	assert.Equal(t, []Event{
		{ID: 3, Sequence: 5},
	}, result)

	result = p.getEventsFrom(5, nil, 3)
	assert.Equal(t, []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}, result)

	result = p.getEventsFrom(5, nil, 4)
	assert.Equal(t, []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}, result)
}

func TestProcessor_Run_Context_Cancelled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	ctx := context.Background()
	events := []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}
	repo.EXPECT().GetLastEvents(ctx, gomock.Any()).Return(events, nil)

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 5,
	})
	_ = p.init(ctx)

	ctx, cancel := context.WithCancel(ctx)
	cancel()

	err := p.run(ctx, nil, nil)
	assert.Nil(t, err)
}

func TestProcessor_Run_Signal(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	ctx := context.Background()
	events := []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}
	repo.EXPECT().GetLastEvents(ctx, gomock.Any()).Return(events, nil)

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 5,
	})
	_ = p.init(ctx)

	repo.EXPECT().GetUnprocessedEvents(ctx, gomock.Any()).
		Return([]Event{
			{ID: 10},
			{ID: 12},
			{ID: 7},
			{ID: 15},
		}, nil)
	repo.EXPECT().UpdateSequences(ctx, []Event{
		{ID: 10, Sequence: 8},
		{ID: 12, Sequence: 9},
		{ID: 7, Sequence: 10},
		{ID: 15, Sequence: 11},
	}).Return(nil)

	signals := make(chan struct{}, 1)
	signals <- struct{}{}

	err := p.run(ctx, signals, nil)
	assert.Nil(t, err)
	assert.Equal(t, uint64(11), p.getLastSequence())
	assert.Equal(t, 0, len(signals))
}

func TestProcessor_Run_Multiple_Signals(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	ctx := context.Background()
	events := []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}
	repo.EXPECT().GetLastEvents(ctx, gomock.Any()).Return(events, nil)

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 5,
	})
	_ = p.init(ctx)

	repo.EXPECT().GetUnprocessedEvents(ctx, gomock.Any()).
		Return([]Event{
			{ID: 10},
			{ID: 12},
			{ID: 7},
			{ID: 15},
		}, nil)
	repo.EXPECT().UpdateSequences(ctx, []Event{
		{ID: 10, Sequence: 8},
		{ID: 12, Sequence: 9},
		{ID: 7, Sequence: 10},
		{ID: 15, Sequence: 11},
	}).Return(nil)

	signals := make(chan struct{}, 3)
	signals <- struct{}{}
	signals <- struct{}{}
	signals <- struct{}{}

	err := p.run(ctx, signals, nil)
	assert.Nil(t, err)
	assert.Equal(t, uint64(11), p.getLastSequence())
	assert.Equal(t, 0, len(signals))
}

func TestProcessor_Run_Fetch__Not_Existed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	ctx := context.Background()
	events := []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}
	repo.EXPECT().GetLastEvents(ctx, gomock.Any()).Return(events, nil)

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 5,
	})
	_ = p.init(ctx)

	respChan := make(chan fetchResponse, 1)

	fetchChan := make(chan fetchRequest, 1)
	fetchChan <- fetchRequest{
		from:         4,
		limit:        1,
		result:       nil,
		responseChan: respChan,
	}

	err := p.run(ctx, nil, fetchChan)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(fetchChan))
	resp := <-respChan
	assert.Equal(t, fetchResponse{
		existed: false,
	}, resp)
}

func TestProcessor_Run_Fetch__Existed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	ctx := context.Background()
	events := []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}
	repo.EXPECT().GetLastEvents(ctx, gomock.Any()).Return(events, nil)

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 5,
	})
	_ = p.init(ctx)

	respChan := make(chan fetchResponse, 1)

	fetchChan := make(chan fetchRequest, 1)
	fetchChan <- fetchRequest{
		from:         5,
		limit:        2,
		result:       nil,
		responseChan: respChan,
	}

	err := p.run(ctx, nil, fetchChan)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(fetchChan))
	resp := <-respChan
	assert.Equal(t, fetchResponse{
		existed: true,
		result: []Event{
			{ID: 3, Sequence: 5},
			{ID: 6, Sequence: 6},
		},
	}, resp)
}

func TestProcessor_Run_Fetch__Wait(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	ctx := context.Background()
	events := []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}
	repo.EXPECT().GetLastEvents(ctx, gomock.Any()).Return(events, nil)

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 5,
	})
	_ = p.init(ctx)

	respChan := make(chan fetchResponse, 3)

	fetchChan := make(chan fetchRequest, 3)
	fetchChan <- fetchRequest{
		from:         8,
		limit:        2,
		result:       nil,
		responseChan: respChan,
	}
	fetchChan <- fetchRequest{
		from:         6,
		limit:        3,
		result:       nil,
		responseChan: respChan,
	}
	fetchChan <- fetchRequest{
		from:         9,
		limit:        3,
		result:       nil,
		responseChan: respChan,
	}

	_ = p.run(ctx, nil, fetchChan)
	_ = p.run(ctx, nil, fetchChan)
	_ = p.run(ctx, nil, fetchChan)

	assert.Equal(t, 0, len(fetchChan))
	assert.Equal(t, 1, len(respChan))

	resp := <-respChan
	assert.Equal(t, fetchResponse{
		existed: true,
		result: []Event{
			{ID: 6, Sequence: 6},
			{ID: 5, Sequence: 7},
		},
	}, resp)

	repo.EXPECT().GetUnprocessedEvents(ctx, gomock.Any()).
		Return([]Event{
			{ID: 10},
			{ID: 12},
		}, nil)

	repo.EXPECT().UpdateSequences(ctx, []Event{
		{ID: 10, Sequence: 8},
		{ID: 12, Sequence: 9},
	}).Return(nil)

	signals := make(chan struct{}, 1)
	signals <- struct{}{}
	_ = p.run(ctx, signals, nil)

	assert.Equal(t, 2, len(respChan))

	resp = <-respChan
	assert.Equal(t, fetchResponse{
		existed: true,
		result: []Event{
			{ID: 10, Sequence: 8},
			{ID: 12, Sequence: 9},
		},
	}, resp)

	resp = <-respChan
	assert.Equal(t, fetchResponse{
		existed: true,
		result: []Event{
			{ID: 10, Sequence: 8},
			{ID: 12, Sequence: 9},
		},
	}, resp)
}

func TestProcessor_Run_Fetch__Wait_And_Wait(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	ctx := context.Background()
	events := []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}
	repo.EXPECT().GetLastEvents(ctx, gomock.Any()).Return(events, nil)

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 5,
	})
	_ = p.init(ctx)

	respChan := make(chan fetchResponse, 3)
	fetchChan := make(chan fetchRequest, 3)
	signals := make(chan struct{}, 1)

	fetchChan <- fetchRequest{
		from:         8,
		limit:        2,
		result:       nil,
		responseChan: respChan,
	}

	_ = p.run(ctx, signals, fetchChan)
	assert.Equal(t, 0, len(respChan))

	signals <- struct{}{}
	repo.EXPECT().GetUnprocessedEvents(ctx, gomock.Any()).
		Return([]Event{
			{ID: 10},
		}, nil)

	repo.EXPECT().UpdateSequences(ctx, []Event{
		{ID: 10, Sequence: 8},
	}).Return(nil)

	_ = p.run(ctx, signals, fetchChan)

	resp := <-respChan
	assert.Equal(t, fetchResponse{
		existed: true,
		result: []Event{
			{ID: 10, Sequence: 8},
		},
	}, resp)

	fetchChan <- fetchRequest{
		from:         10,
		limit:        1,
		result:       nil,
		responseChan: respChan,
	}
	fetchChan <- fetchRequest{
		from:         9,
		limit:        3,
		result:       nil,
		responseChan: respChan,
	}

	_ = p.run(ctx, signals, fetchChan)
	_ = p.run(ctx, signals, fetchChan)
	signals <- struct{}{}
	repo.EXPECT().GetUnprocessedEvents(ctx, gomock.Any()).
		Return([]Event{
			{ID: 19},
		}, nil)

	repo.EXPECT().UpdateSequences(ctx, []Event{
		{ID: 19, Sequence: 9},
	}).Return(nil)
	_ = p.run(ctx, signals, fetchChan)

	assert.Equal(t, 2, len(respChan))

	resp = <-respChan
	assert.Equal(t, fetchResponse{
		existed: true,
		result: []Event{
			{ID: 19, Sequence: 9},
		},
	}, resp)

	resp = <-respChan
	assert.Equal(t, fetchResponse{
		existed: true,
		result: []Event{
			{ID: 19, Sequence: 9},
		},
	}, resp)
}
