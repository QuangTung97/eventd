package eventd

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

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
