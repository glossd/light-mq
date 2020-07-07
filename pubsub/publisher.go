package pubsub

import (
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/pubsub/gate"
	"github.com/gl-ot/light-mq/pubsub/message/msgrepo"
)

// Stores the message on disk then
// publishes message to all open gates
func Publish(topic string, message []byte) error {
	if topic == "" {
		return emptyTopicError
	}

	if err := config.MkDirTopic(topic); err != nil {
		return err
	}

	offset, err := msgrepo.Store(topic, message)
	if err != nil {
		return err
	}
	go stream.SendMessage(topic, &msgrepo.Message{Offset: offset, Body: message})
	return nil
}
