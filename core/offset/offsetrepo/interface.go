package offsetrepo

type Storage interface {
	// Returns latest offset subscriber group.
	// Returns nil in case offset doesn't exist
	// todo maybe Get doesn't need to return error
	Get(sg *SubscriberGroup) *uint64
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
