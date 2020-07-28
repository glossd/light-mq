package domain

import "fmt"

// Represents a group of subscribers for Topic
type SGroup struct {
	// Name of the group
	Group string
	Topic string
}

// todo use it for validation
func NewSGroup(group, topic string) *SGroup {
	panic("implement me")
}

func (sg SGroup) String() string {
	return fmt.Sprintf("SGroup{topic: %s, group: %s}", sg.Topic, sg.Group)
}

func (sg SGroup) AsKey() string {
	return sg.Topic + sg.Group
}

