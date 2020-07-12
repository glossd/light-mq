package offsetrepo

import "fmt"

type Storage interface {
	// Returns latest offset subscriber group.
	// Returns nil in case offset doesn't exist
	// todo maybe Get doesn't need to return error
	Get(sg *SubscriberGroup) (*uint64, error)
	// Stores new offset
	Update(sg *SubscriberGroup, newOffset uint64) error
}

type SubscriberGroup struct {
	Group string
	Topic string
}

func (sg SubscriberGroup) asKey() string {
	return sg.Topic + sg.Group
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
	fs := FileStorage{offsets: make(map[string]*uint64)}
	fs.fillOffsetsOnStartUp()
	SubscriberOffsetStorage = &fs

}
