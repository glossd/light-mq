package domain

import "fmt"

type SubscriberID string

func (id SubscriberID) Short() string {
	return string(id)[:7]
}

// Subscriber is a member of SGroup
type Subscriber struct {
	ID SubscriberID
	SGroup
}

func (s Subscriber) String() string {
	return fmt.Sprintf("Subscriber{id: %s, topic: %s, group: %s}", s.ID.Short(), s.Topic, s.Group)
}
