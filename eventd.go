package eventd

import "time"

const (
	// DefaultGetLastEventsLimit ...
	DefaultGetLastEventsLimit uint64 = 1000
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
}

// Runner ...
type Runner struct {
}

// New ...
func New() *Runner {
	return &Runner{}
}
