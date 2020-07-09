package pubsub

import (
	"github.com/gl-ot/light-mq/pubsub/gate"
	"github.com/gl-ot/light-mq/pubsub/message/msgrepo"
)

// Stores the message on disk then
// publishes message to all open gates
func Publish(topic string, message []byte) error {
	if topic == "" {
		return emptyTopicError
	}

	offset, err := msgrepo.Store(topic, message)
	if err != nil {
		return err
	}
	go gate.SendMessage(topic, &msgrepo.Message{Offset: offset, Body: message})
	return nil
}
