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

	repo.EXPECT().GetLastEvents(DefaultGetLastEventsLimit).
		Return(nil, errors.New("get-last-events-error"))

	p := newProcessor(repo, defaultRunnerOpts())
	err := p.init()
	assert.Equal(t, errors.New("get-last-events-error"), err)
}

func TestProcessor_Init__Without_Last_Events(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)

	repo.EXPECT().GetLastEvents(DefaultGetLastEventsLimit).Return(nil, nil)

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
		{
			ID:       10,
			Sequence: 5,
		},
		{
			ID:       9,
			Sequence: 6,
		},
		{
			ID:       11,
			Sequence: 7,
		},
	}
	repo.EXPECT().GetLastEvents(DefaultGetLastEventsLimit).
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
		{
			ID:       10,
			Sequence: 5,
		},
		{
			ID:       9,
			Sequence: 6,
		},
		{
			ID:       11,
			Sequence: 7,
		},
	}
	repo.EXPECT().GetLastEvents(uint64(4)).
		Return(events, nil)

	p := newProcessor(repo, runnerOpts{
		getLastEventsLimit: 4,
		storedEventSize:    3,
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
		{
			ID:       10,
			Sequence: 5,
		},
		{
			ID:       9,
			Sequence: 6,
		},
		{
			ID:       11,
			Sequence: 7,
		},
	}
	repo.EXPECT().GetLastEvents(uint64(4)).
		Return(events, nil)

	p := newProcessor(repo, runnerOpts{
		getLastEventsLimit: 4,
		storedEventSize:    2,
	})
	err := p.init()

	assert.Equal(t, nil, err)
	assert.Equal(t, events[1:], p.currentEvents())
}
