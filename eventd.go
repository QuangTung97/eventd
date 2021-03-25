package eventd

import "time"

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
	GetLastEvents(limit uint64) ([]Event, error)
	GetUnprocessedEvents(limit uint64) ([]Event, error)

	UpdateSequences(events []Event) error
}

// Runner ...
type Runner struct {
}

// New ...
func New() *Runner {
	return &Runner{}
}
