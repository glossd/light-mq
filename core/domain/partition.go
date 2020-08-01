package domain

import "fmt"

// Represents a group of subscribers for Topic
type Partition struct {
	ID int
	Topic string
}

func (p Partition) String() string {
	return fmt.Sprintf("Partition{id: %d, topic: %s}", p.ID, p.Topic)
}
