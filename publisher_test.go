package eventd

import (
	"context"
	"errors"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"testing"
)

// PUBLISHER WAIT LIST

func TestPublisherWaitList_Single(t *testing.T) {
	wl := newPublisherWaitList(1)
	var expected chan<- uint64

	wait1 := make(chan uint64)
	wl.put(20, wait1)

	resp, ok := wl.get(20)

	assert.True(t, ok)
	expected = wait1
	assert.Equal(t, expected, resp)
}

func TestPublisherWaitList_Double(t *testing.T) {
	wl := newPublisherWaitList(2)
	var expected chan<- uint64

	wait1 := make(chan uint64)
	wait2 := make(chan uint64)
	wl.put(20, wait1)
	wl.put(40, wait2)

	resp, ok := wl.get(20)
	assert.True(t, ok)
	expected = wait1
	assert.Equal(t, expected, resp)

	resp, ok = wl.get(40)
	assert.True(t, ok)
	expected = wait2
	assert.Equal(t, expected, resp)

	resp, ok = wl.get(5)
	assert.False(t, ok)
	expected = nil
	assert.Equal(t, expected, resp)
}

func TestPublisherWaitList_Exceeded(t *testing.T) {
	wl := newPublisherWaitList(2)
	var expected chan<- uint64

	wait1 := make(chan uint64)
	wait2 := make(chan uint64)
	wait3 := make(chan uint64)
	wl.put(20, wait1)
	wl.put(40, wait2)
	wl.put(50, wait3)

	resp, ok := wl.get(20)
	assert.False(t, ok)
	expected = nil
	assert.Equal(t, expected, resp)

	resp, ok = wl.get(40)
	assert.True(t, ok)
	expected = wait2
	assert.Equal(t, expected, resp)
}

func TestPublisherWaitList_Delete(t *testing.T) {
	wl := newPublisherWaitList(2)
	var expected chan<- uint64

	wait1 := make(chan uint64)
	wait2 := make(chan uint64)
	wait3 := make(chan uint64)
	wl.put(20, wait1)
	wl.put(40, wait2)

	wl.delete(20)

	resp, ok := wl.get(20)
	assert.False(t, ok)
	expected = nil
	assert.Equal(t, expected, resp)

	wl.put(50, wait3)

	resp, ok = wl.get(40)
	assert.True(t, ok)
	expected = wait2
	assert.Equal(t, expected, resp)

	resp, ok = wl.get(50)
	assert.True(t, ok)
	expected = wait3
	assert.Equal(t, expected, resp)
}

// PUBLISHER PROCESSED LIST
func TestPublisherProcessedList_Single(t *testing.T) {
	wl := newPublisherProcessedList(1)
	wl.put(20, 100)

	resp, ok := wl.get(20)

	assert.True(t, ok)
	assert.Equal(t, uint64(100), resp)
}

func TestPublisherProcessedList_Double(t *testing.T) {
	wl := newPublisherProcessedList(2)

	wl.put(20, 100)
	wl.put(40, 200)

	resp, ok := wl.get(20)
	assert.True(t, ok)
	assert.Equal(t, uint64(100), resp)

	resp, ok = wl.get(40)
	assert.True(t, ok)
	assert.Equal(t, uint64(200), resp)

	resp, ok = wl.get(5)
	assert.False(t, ok)
	assert.Equal(t, uint64(0), resp)
}

func TestPublisherProcessedList_Exceeded(t *testing.T) {
	wl := newPublisherProcessedList(2)

	wl.put(20, 100)
	wl.put(40, 200)
	wl.put(50, 300)

	resp, ok := wl.get(20)
	assert.False(t, ok)
	assert.Equal(t, uint64(0), resp)

	resp, ok = wl.get(40)
	assert.True(t, ok)
	assert.Equal(t, uint64(200), resp)
}

// PUBLISHER RUNNER

func TestPublisherRunner_Init_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	p := NewMockPublisher(ctrl)

	ctx := context.Background()

	repo.EXPECT().GetLastSequence(ctx, PublisherID(5)).
		Return(uint64(0), errors.New("get-last-seq-error"))

	r := newPublisherRunner(
		5, repo, p, nil,
		publisherOpts{
			waitListLimit: 3,
		})

	err := r.init(ctx)
	assert.Equal(t, errors.New("get-last-seq-error"), err)
	assert.False(t, r.isFetching())
}

func TestPublisherRunner_Fetch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	p := NewMockPublisher(ctrl)

	repo.EXPECT().GetLastSequence(gomock.Any(), gomock.Any()).
		Return(uint64(50), nil)

	fetchChan := make(chan fetchRequest, 1)

	r := newPublisherRunner(
		5, repo, p, fetchChan,
		publisherOpts{
			waitListLimit: 3,
			publishLimit:  5,
		})

	ctx := context.Background()
	err := r.init(ctx)
	assert.Equal(t, nil, err)

	r.fetch()

	assert.Equal(t, 1, len(fetchChan))
	req := <-fetchChan
	assert.Equal(t, uint64(51), req.from)
	assert.Equal(t, uint64(5), req.limit)
	assert.Equal(t, 5, cap(req.result))
	assert.Equal(t, 0, len(req.result))
	assert.NotNil(t, req.responseChan)
	assert.True(t, r.isFetching())
}

func TestPublisherRunner_Run__Publish_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	p := NewMockPublisher(ctrl)

	repo.EXPECT().GetLastSequence(gomock.Any(), gomock.Any()).
		Return(uint64(50), nil)

	fetchChan := make(chan fetchRequest, 1)

	r := newPublisherRunner(
		5, repo, p, fetchChan,
		publisherOpts{
			waitListLimit: 3,
			publishLimit:  5,
		})

	ctx := context.Background()
	_ = r.init(ctx)

	r.fetch()
	req := <-fetchChan
	req.responseChan <- fetchResponse{
		existed: true,
		result: []Event{
			{ID: 5, Sequence: 20},
			{ID: 3, Sequence: 21},
		},
	}

	p.EXPECT().Publish(ctx, []Event{
		{ID: 5, Sequence: 20},
		{ID: 3, Sequence: 21},
	}).Return(errors.New("publish-error"))

	err := r.run(ctx, nil)
	assert.Equal(t, errors.New("publish-error"), err)
}

func TestPublisherRunner_Run__Save_Last_Seq_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	p := NewMockPublisher(ctrl)

	repo.EXPECT().GetLastSequence(gomock.Any(), gomock.Any()).
		Return(uint64(13), nil)

	fetchChan := make(chan fetchRequest, 1)

	r := newPublisherRunner(
		11, repo, p, fetchChan,
		publisherOpts{
			processedListLimit: 4,
			waitListLimit:      3,
			publishLimit:       5,
		})

	ctx := context.Background()
	_ = r.init(ctx)

	r.fetch()
	req := <-fetchChan
	req.responseChan <- fetchResponse{
		existed: true,
		result: []Event{
			{ID: 5, Sequence: 20},
			{ID: 3, Sequence: 21},
		},
	}

	p.EXPECT().Publish(ctx, []Event{
		{ID: 5, Sequence: 20},
		{ID: 3, Sequence: 21},
	}).Return(nil)
	repo.EXPECT().SaveLastSequence(ctx, PublisherID(11), uint64(21)).
		Return(errors.New("save-last-seq-error"))

	err := r.run(ctx, nil)
	assert.Equal(t, errors.New("save-last-seq-error"), err)
	assert.Equal(t, uint64(13), r.lastSequence)
}

func TestPublisherRunner_Run__Context_Cancelled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	p := NewMockPublisher(ctrl)

	repo.EXPECT().GetLastSequence(gomock.Any(), gomock.Any()).
		Return(uint64(50), nil)

	fetchChan := make(chan fetchRequest, 1)

	r := newPublisherRunner(
		11, repo, p, fetchChan,
		publisherOpts{
			waitListLimit: 3,
			publishLimit:  5,
		})

	ctx := context.Background()
	_ = r.init(ctx)
	r.fetch()

	ctx, cancel := context.WithCancel(ctx)
	cancel()

	err := r.run(ctx, nil)
	assert.Equal(t, context.Canceled, err)
}

func TestPublisherRunner_Run__Response_To_Wait__After_Recv_Resp_From_Processor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	p := NewMockPublisher(ctrl)

	repo.EXPECT().GetLastSequence(gomock.Any(), gomock.Any()).
		Return(uint64(50), nil)
	p.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil)
	repo.EXPECT().SaveLastSequence(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	fetchChan := make(chan fetchRequest, 1)

	r := newPublisherRunner(
		11, repo, p, fetchChan,
		publisherOpts{
			processedListLimit: 4,
			waitListLimit:      3,
			publishLimit:       5,
		})

	ctx := context.Background()
	_ = r.init(ctx)

	r.fetch()

	waitRespChan := make(chan uint64, 1)
	waitReqChan := make(chan waitRequest, 1)
	waitReqChan <- waitRequest{
		eventID:      3,
		responseChan: waitRespChan,
	}

	_ = r.run(ctx, waitReqChan)

	req := <-fetchChan
	req.responseChan <- fetchResponse{
		existed: true,
		result: []Event{
			{ID: 5, Sequence: 20},
			{ID: 3, Sequence: 21},
		},
	}

	err := r.run(ctx, waitReqChan)

	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(waitReqChan))
	assert.Equal(t, 1, len(waitRespChan))
	resp := <-waitRespChan
	assert.Equal(t, uint64(21), resp)

	assert.Equal(t, uint64(21), r.lastSequence)
}

func TestPublisherRunner_Run__Response_To_Wait__Right_After_Wait_Request(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := NewMockRepository(ctrl)
	p := NewMockPublisher(ctrl)

	repo.EXPECT().GetLastSequence(gomock.Any(), gomock.Any()).
		Return(uint64(50), nil)
	p.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil)
	repo.EXPECT().SaveLastSequence(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	fetchChan := make(chan fetchRequest, 1)

	r := newPublisherRunner(
		11, repo, p, fetchChan,
		publisherOpts{
			processedListLimit: 4,
			waitListLimit:      3,
			publishLimit:       5,
		})

	ctx := context.Background()
	_ = r.init(ctx)

	r.fetch()

	req := <-fetchChan
	req.responseChan <- fetchResponse{
		existed: true,
		result: []Event{
			{ID: 5, Sequence: 20},
			{ID: 3, Sequence: 21},
		},
	}

	err := r.run(ctx, nil)

	waitRespChan := make(chan uint64, 1)
	waitReqChan := make(chan waitRequest, 1)
	waitReqChan <- waitRequest{
		eventID:      3,
		responseChan: waitRespChan,
	}

	_ = r.run(ctx, waitReqChan)

	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(waitReqChan))
	assert.Equal(t, 1, len(waitRespChan))
	resp := <-waitRespChan
	assert.Equal(t, uint64(21), resp)
}
