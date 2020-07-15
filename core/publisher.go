package core

import (
	"github.com/gl-ot/light-mq/core/gate"
	"github.com/gl-ot/light-mq/core/message/msgservice"
	log "github.com/sirupsen/logrus"
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
	msg := msgservice.Message{Offset: offset, Body: message}
	log.Tracef("Publisher sending %s", msg)

	// todo run it another thread. Does it block?
	gate.SendMessage(topic, &msg)

	return nil
}
