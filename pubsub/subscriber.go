package pubsub

import (
	"context"
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/pubsub/offset/offsetService"
	"github.com/gl-ot/light-mq/pubsub/offset/offsetStorage"
	"github.com/gl-ot/light-mq/pubsub/topicStorage"
	log "github.com/sirupsen/logrus"
)

func NewSub(topic string, group string) (*Subscriber, error) {
	if topic == "" {
		return nil, InputError{Msg: "topic can't be empty"}
	}
	if group == "" {
		return nil, InputError{Msg: "group can't be empty"}
	}
	if err := config.CreateTopicDir(topic); err != nil {
		return nil, err
	}
	s := &Subscriber{
		topic:   topic,
		group:   group,
		channel: make(chan []byte),
	}
	subs, ok := topicSubs.LoadOrStore(topic, map[*Subscriber]bool{s: true})
	if ok {
		subs.(map[*Subscriber]bool)[s] = true
	}
	return s, nil
}

type Subscriber struct {
	topic   string
	group   string
	channel chan []byte
}

/*
	1) Obtains offset of the subscriber
	2) Sends all missed messages
	3) Waits for new ones
*/
func (s *Subscriber) Subscribe(ctx context.Context, handler func([]byte)) error {
	offset, err := offsetService.ComputeSubscriberOffset(&offsetStorage.SubscriberGroup{Topic: s.topic, Group: s.group})
	if err != nil {
		return err
	}

	messages, err := topicStorage.GetMessages(s.topic, offset)
	if err != nil {
		return err
	}

	for _, message := range messages {
		handleMessage(s, message, handler)
	}

	for {
		select {
		case msg := <-s.channel:
			handleMessage(s, msg, handler)
		case <-ctx.Done():
			return nil
		}
	}
}

func handleMessage(s *Subscriber, message []byte, handler func([]byte)) {
	offsetService.IncrementOffset(&offsetStorage.SubscriberGroup{Topic: s.topic, Group: s.group})
	handler(message)
}

func (s *Subscriber) Close() {
	if s != nil {
		log.Debugf("Lost subscriber on topic %s", s.topic)
		close(s.channel)
		subs, ok := topicSubs.Load(s.topic)
		if ok {
			delete(subs.(map[*Subscriber]bool), s)
		} else {
			log.Warnf("Didn't find subscriber in topic subscribers: topic=%s", s.topic)
		}
	}
}
