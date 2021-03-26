package eventd

import (
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"testing"
	"time"
)

func TestPublisherOptions(t *testing.T) {
	options := defaultPublisherOptions
	expected := publisherOpts{
		processedListLimit: DefaultGetEventsLimit,
		waitListLimit:      DefaultGetEventsLimit,
		publishLimit:       DefaultGetEventsLimit,
	}
	assert.Equal(t, expected, options)

	WithProcessedListLimit(20)(&options)
	WithWaitListLimit(40)(&options)
	WithPublishLimit(50)(&options)

	expected = publisherOpts{
		processedListLimit: 20,
		waitListLimit:      40,
		publishLimit:       50,
	}
	assert.Equal(t, expected, options)
}

func TestRunnerOptions(t *testing.T) {
	options := defaultRunnerOpts
	expected := runnerOpts{
		getEventsLimit:     DefaultGetEventsLimit,
		retryDuration:      60 * time.Second,
		storedEventSize:    DefaultGetEventsLimit,
		errorSleepDuration: 30 * time.Second,
		publishers:         map[PublisherID]registeredPublisher{},
		logger:             zap.NewNop(),
	}
	assert.Equal(t, expected, options)

	logger, err := zap.NewProduction()
	assert.Nil(t, err)
	WithLogger(logger)(&options)

	assert.Same(t, logger, options.logger)

	publisher := &MockPublisher{}
	waitChan := make(chan waitRequest)

	WithPublisher(100, publisher, waitChan, WithProcessedListLimit(70))(&options)
	assert.Equal(t, 1, len(options.publishers))

	expectedPublisherOpts := publisherOpts{
		processedListLimit: 70,
		waitListLimit:      DefaultGetEventsLimit,
		publishLimit:       DefaultGetEventsLimit,
	}
	assert.Equal(t, expectedPublisherOpts, options.publishers[100].options)
}

func TestComputeRunnerOptions(t *testing.T) {
	logger, err := zap.NewProduction()
	assert.Nil(t, err)

	publisher := &MockPublisher{}
	waitChan := make(chan waitRequest)

	options := computeRunnerOpts(
		WithLogger(logger),
		WithGetEventsLimit(55),
		WithRetryDuration(25*time.Second),
		WithStoredEventsSize(66),
		WithErrorSleepDuration(20*time.Second),
		WithPublisher(100, publisher, waitChan,
			WithProcessedListLimit(70),
		),
	)

	assert.Same(t, logger, options.logger)
	assert.Equal(t, uint64(55), options.getEventsLimit)
	assert.Equal(t, 25*time.Second, options.retryDuration)
	assert.Equal(t, uint64(66), options.storedEventSize)
	assert.Equal(t, 20*time.Second, options.errorSleepDuration)

	expectedPublisherOpts := publisherOpts{
		processedListLimit: 70,
		waitListLimit:      DefaultGetEventsLimit,
		publishLimit:       DefaultGetEventsLimit,
	}
	assert.Equal(t, 1, len(options.publishers))
	assert.Equal(t, expectedPublisherOpts, options.publishers[100].options)
}
