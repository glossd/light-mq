package offsetStorage

import "fmt"

type Storage interface {
	// Returns latest offset
	GetLatest(key *SubscriberGroup) (int, error)
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
	SubscriberOffsetStorage = boltStorage{db: createBoltDb()}
}
