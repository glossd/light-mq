package log

import (
	"context"
	log "github.com/sirupsen/logrus"
)

type Subscriber struct {
	topic   string
	channel chan []byte
}

func NewSub(topic string) *Subscriber {
	s := &Subscriber{
		topic:   topic,
		channel: make(chan []byte),
	}
	subs, ok := topicSubs.LoadOrStore(topic, map[*Subscriber]bool{s: true})
	if ok {
		subs.(map[*Subscriber]bool)[s] = true
	}
	return s
}

func (s *Subscriber) Subscribe(ctx context.Context, f func([]byte)) {
	for {
		select {
		case msg := <-s.channel:
			f(msg)
		case <-ctx.Done():
			return
		}
	}
}

func (s *Subscriber) Close() {
	log.Debugf("Lost subscriber on topic %s", s.topic)
	close(s.channel)
	subs, ok := topicSubs.Load(s.topic)
	if ok {
		delete(subs.(map[*Subscriber]bool), s)
	} else {
		log.Warnf("Didn't find subscriber in topic subscribers: topic=%s", s.topic)
	}
}
