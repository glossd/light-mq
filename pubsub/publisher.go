package pubsub

import (
	"github.com/gl-ot/light-mq/pubsub/gate"
	"github.com/gl-ot/light-mq/pubsub/message/msgservice"
)

// Stores the message on disk then
// publishes message to all open gates
func Publish(topic string, message []byte) error {
	if topic == "" {
		return emptyTopicError
	}

	offset, err := msgservice.Store(topic, message)
	if err != nil {
		return err
	}
	go gate.SendMessage(topic, &msgservice.Message{Offset: offset, Body: message})
	return nil
}
