package core

import (
	"github.com/gl-ot/light-mq/core/gates"
	"github.com/gl-ot/light-mq/core/message/msgservice"
	log "github.com/sirupsen/logrus"
	"sync"
)

var topicLocks sync.Map

// Stores the message on disk then
// publishes message to all open gates
func Publish(topic string, message []byte) error {
	if topic == "" {
		return emptyTopicError
	}
	if len(message) == 0 {
		return InputError{Msg: "message can't be empty"}
	}

	// this is a way to kill performance and maintain order of messages.
	// todo only put lock when saving message to log
	mutex, _ := topicLocks.LoadOrStore(topic, &sync.Mutex{})
	mutex.(*sync.Mutex).Lock()
	defer mutex.(*sync.Mutex).Unlock()

	log.Tracef("Publisher got new message %s", message)

	offset, err := msgservice.Store(topic, message)
	if err != nil {
		return err
	}
	msg := msgservice.Message{Offset: offset, Body: message}
	log.Tracef("Publisher sending %s", msg)

	// todo run it another thread. Does it block?
	gates.SendMessage(topic, &msg)

	return nil
}
