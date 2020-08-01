package core

import (
	"github.com/gl-ot/light-mq/core/gate"
	"github.com/gl-ot/light-mq/core/record/recordstore"
	"github.com/gl-ot/light-mq/core/lockutil"
	log "github.com/sirupsen/logrus"
)

var topicLocks = lockutil.NewTopicLock()

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
	topicLocks.Lock(topic)
	defer topicLocks.Unlock(topic)
	log.Tracef("Publisher got new message %s", message)

	record, err := recordstore.Store(topic, message)
	if err != nil {
		return err
	}

	// todo run it another thread. Does it block?
	gate.SendRecord(record)

	return nil
}
