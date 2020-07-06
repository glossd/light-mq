package pubsub

import (
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/pubsub/message/msgRepo"
	"github.com/gl-ot/light-mq/pubsub/stream"
)

/*
	1) Stores the message on disk
	2) Publishes the message to all subscribers
*/
func Publish(topic string, message []byte) error {
	if topic == "" {
		return emptyTopicError
	}

	if err := config.MkDirTopic(topic); err != nil {
		return err
	}

	offset, err := msgRepo.StoreMessage(topic, message)
	if err != nil {
		return err
	}
	go stream.SendMessageToOpenedGates(topic, &msgRepo.Message{Offset: offset, Body: message})
	return nil
}
