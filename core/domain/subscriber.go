package domain

type SubscriberID string

// Subscriber is a member of SGroup
type Subscriber struct {
	ID SubscriberID
	SGroup
}

func (s Subscriber) String() string {
	return s.SGroup.String()
}
