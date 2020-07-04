package pubsub

import (
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/pubsub/topicStorage"
)

/*
	1) Stores the message on disk
	2) Publishes the message to all subscribers
*/
func Publish(topic string, message []byte) error {
	if err := config.CreateTopicDir(topic); err != nil {
		return err
	}
	if err := topicStorage.StoreMessage(topic, message); err != nil {
		return err
	}
	// concurrently???
	notifySubscribers(topic, message)
	return nil
}

func notifySubscribers(topic string, message []byte) {
	// todo send by subscriber group
	subs, ok := topicSubs.Load(topic)
	if ok {
		for s := range subs.(map[*Subscriber]bool) {
			s.channel <- message
		}
	}
}
