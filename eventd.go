package eventd

import (
	"context"
	"time"
)

const (
	// DefaultGetEventsLimit ...
	DefaultGetEventsLimit uint64 = 1000
)

// Event ...
type Event struct {
	ID        uint64
	Sequence  uint64
	Data      string
	CreatedAt time.Time
}

//go:generate mockgen -destination=repository_mocks.go -self_package=github.com/QuangTung97/eventd -package=eventd . Repository

// Repository ...
type Repository interface {
	GetLastEvents(ctx context.Context, limit uint64) ([]Event, error)
	GetUnprocessedEvents(ctx context.Context, limit uint64) ([]Event, error)

	UpdateSequences(ctx context.Context, events []Event) error

	GetLastSequence(ctx context.Context, id PublisherID) (uint64, error)
	SaveLastSequence(ctx context.Context, id PublisherID, seq uint64) error
}

// PublisherID ...
type PublisherID uint32

//go:generate mockgen -destination=publisher_mocks.go -self_package=github.com/QuangTung97/eventd -package=eventd . Publisher

// Publisher ...
type Publisher interface {
	Publish(ctx context.Context, events []Event) error
}

// Runner ...
type Runner struct {
}

// New ...
func New() *Runner {
	return &Runner{}
}
