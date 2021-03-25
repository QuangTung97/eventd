package eventd

import (
	"errors"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestProcessor_Init__GetLastEvents_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)

	repo.EXPECT().GetLastEvents(DefaultGetEventsLimit).
		Return(nil, errors.New("get-last-events-error"))

	p := newProcessor(repo, defaultRunnerOpts())
	err := p.init()
	assert.Equal(t, errors.New("get-last-events-error"), err)
}

func TestProcessor_Init__Without_Last_Events(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)

	repo.EXPECT().GetLastEvents(DefaultGetEventsLimit).Return(nil, nil)

	p := newProcessor(repo, defaultRunnerOpts())
	err := p.init()

	assert.Equal(t, nil, err)
	assert.Equal(t, []Event(nil), p.currentEvents())
}

func TestProcessor_Init__With_Last_Events(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)

	events := []Event{
		{ID: 10, Sequence: 5},
		{ID: 9, Sequence: 6},
		{ID: 11, Sequence: 7},
	}
	repo.EXPECT().GetLastEvents(DefaultGetEventsLimit).
		Return(events, nil)

	p := newProcessor(repo, defaultRunnerOpts())
	err := p.init()

	assert.Equal(t, nil, err)
	assert.Equal(t, events, p.currentEvents())
}

func TestProcessor_Init__With_Last_3_Events__Limit_3(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)

	events := []Event{
		{ID: 10, Sequence: 5},
		{ID: 9, Sequence: 6},
		{ID: 11, Sequence: 7},
	}
	repo.EXPECT().GetLastEvents(uint64(4)).
		Return(events, nil)

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 3,
	})
	err := p.init()

	assert.Equal(t, nil, err)
	assert.Equal(t, events, p.currentEvents())
}

func TestProcessor_Init__With_Last_3_Events__Limit_2(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)

	events := []Event{
		{ID: 10, Sequence: 5},
		{ID: 9, Sequence: 6},
		{ID: 11, Sequence: 7},
	}
	repo.EXPECT().GetLastEvents(uint64(4)).
		Return(events, nil)

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 2,
	})
	err := p.init()

	assert.Equal(t, nil, err)
	assert.Equal(t, events[1:], p.currentEvents())
}

func TestProcessor_Signal__GetUnprocessed_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	repo.EXPECT().GetLastEvents(gomock.Any()).Return(nil, nil)
	repo.EXPECT().GetUnprocessedEvents(uint64(4)).
		Return(nil, errors.New("get-unprocessed-error"))

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 3,
	})
	_ = p.init()

	err := p.signal()
	assert.Equal(t, errors.New("get-unprocessed-error"), err)
}

func TestProcessor_Signal__Empty_Unprocessed__Without_Stored(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	repo.EXPECT().GetLastEvents(gomock.Any()).Return(nil, nil)
	repo.EXPECT().GetUnprocessedEvents(uint64(4)).
		Return(nil, nil)

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 3,
	})
	_ = p.init()

	err := p.signal()
	assert.Equal(t, nil, err)
	assert.Equal(t, []Event(nil), p.currentEvents())
}

func TestProcessor_Signal__Empty_Unprocessed__With_Stored(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	events := []Event{
		{ID: 10, Sequence: 5},
		{ID: 9, Sequence: 6},
		{ID: 11, Sequence: 7},
	}
	repo.EXPECT().GetLastEvents(gomock.Any()).Return(events, nil)
	repo.EXPECT().GetUnprocessedEvents(uint64(4)).
		Return(nil, nil)

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 3,
	})
	_ = p.init()

	err := p.signal()
	assert.Equal(t, nil, err)
	assert.Equal(t, events, p.currentEvents())
}

func TestProcessor_Signal__4_Unprocessed__Update_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	repo.EXPECT().GetLastEvents(gomock.Any()).Return(nil, nil)
	repo.EXPECT().GetUnprocessedEvents(gomock.Any()).
		Return([]Event{
			{ID: 10},
			{ID: 12},
			{ID: 7},
			{ID: 15},
		}, nil)

	repo.EXPECT().UpdateSequences([]Event{
		{ID: 10, Sequence: 1},
		{ID: 12, Sequence: 2},
		{ID: 7, Sequence: 3},
		{ID: 15, Sequence: 4},
	}).Return(errors.New("update-sequences-error"))

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 3,
	})
	_ = p.init()

	err := p.signal()
	assert.Equal(t, errors.New("update-sequences-error"), err)
	assert.Equal(t, []Event(nil), p.currentEvents())
}

func TestProcessor_Signal__4_Unprocessed__Update_OK(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	repo.EXPECT().GetLastEvents(gomock.Any()).Return(nil, nil)
	repo.EXPECT().GetUnprocessedEvents(gomock.Any()).
		Return([]Event{
			{ID: 10},
			{ID: 12},
			{ID: 7},
			{ID: 15},
		}, nil)

	repo.EXPECT().UpdateSequences([]Event{
		{ID: 10, Sequence: 1},
		{ID: 12, Sequence: 2},
		{ID: 7, Sequence: 3},
		{ID: 15, Sequence: 4},
	}).Return(nil)

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 3,
	})
	_ = p.init()

	err := p.signal()
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
	events := []Event{
		{ID: 3, Sequence: 5},
		{ID: 6, Sequence: 6},
		{ID: 5, Sequence: 7},
	}
	repo.EXPECT().GetLastEvents(gomock.Any()).Return(events, nil)
	repo.EXPECT().GetUnprocessedEvents(gomock.Any()).
		Return([]Event{
			{ID: 10},
			{ID: 12},
			{ID: 7},
			{ID: 15},
		}, nil)

	repo.EXPECT().UpdateSequences([]Event{
		{ID: 10, Sequence: 8},
		{ID: 12, Sequence: 9},
		{ID: 7, Sequence: 10},
		{ID: 15, Sequence: 11},
	}).Return(nil)

	p := newProcessor(repo, runnerOpts{
		getEventsLimit:  4,
		storedEventSize: 5,
	})
	_ = p.init()

	err := p.signal()
	assert.Equal(t, nil, err)
	assert.Equal(t, []Event{
		{ID: 5, Sequence: 7},
		{ID: 10, Sequence: 8},
		{ID: 12, Sequence: 9},
		{ID: 7, Sequence: 10},
		{ID: 15, Sequence: 11},
	}, p.currentEvents())
}
