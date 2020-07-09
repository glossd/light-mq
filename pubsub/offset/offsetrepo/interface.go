package offsetrepo

import "fmt"

type Storage interface {
	// Returns latest offset.
	// Returns nil in case subscriber's offset doesn't exist
	// todo maybe GetLatest doesn't need to return error
	GetLatest(key *SubscriberGroup) (*int, error)
	// Stores new offset
	Store(key *SubscriberGroup, newOffset int) error
}

type SubscriberGroup struct {
	Group string
	Topic string
}

type SubscriberGroupNotFound struct {
	*SubscriberGroup
}

func (e SubscriberGroupNotFound) Error() string {
	return fmt.Sprintf("not found group: %s, topic: %s", e.SubscriberGroup.Group, e.SubscriberGroup.Topic)
}

var SubscriberOffsetStorage Storage

func init() {
	InitStorage()
}

func InitStorage() {
	SubscriberOffsetStorage = boltStorage{db: createBoltDb()}
}
