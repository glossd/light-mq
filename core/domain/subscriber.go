package domain

// Subscriber is a member of SGroup
type Subscriber struct {
	UUID string
	Group SGroup
}

func (s Subscriber) String() string {
	return s.Group.String()
}
