package core

import (
	"context"
	"fmt"
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/core/gates"
	"github.com/gl-ot/light-mq/core/message/msgservice"
	"github.com/gl-ot/light-mq/core/offset/offsetrepo"
	log "github.com/sirupsen/logrus"
)

type Subscriber struct {
	Topic string
	Group string
	gate  *gates.Gate
}

func (s Subscriber) String() string {
	return fmt.Sprintf("Sub{%s %s}", s.Topic, s.Group)
}

func NewSub(topic string, group string) (*Subscriber, error) {
	if topic == "" {
		return nil, emptyTopicError
	}
	if group == "" {
		return nil, InputError{Msg: "Group can't be empty"}
	}

	err := config.MkDirGroup(topic, group)
	if err != nil {
		return nil, err
	}

	log.Debugf("New subscriber: topic=%s, group=%s", topic, group)

	return &Subscriber{
		Topic: topic,
		Group: group,
		gate:  gates.New(topic, group),
	}, nil
}

// Invokes handler on every new message.
// Blocks until context is canceled.
func (s *Subscriber) Subscribe(ctx context.Context, handler func([]byte) error) error {
	// todo probably race condition on two subscribers with the same Group
	offset:= offsetrepo.SubscriberOffsetStorage.Get(&offsetrepo.SubscriberGroup{Topic: s.Topic, Group: s.Group})

	s.gate.Open()

	var fromOffset uint64
	if offset != nil {
		fromOffset = *offset
	}
	// todo int to uint64
	messages, err := msgservice.GetAllFrom(s.Topic, fromOffset)
	if err != nil {
		return err
	}
	log.Debugf("%s received %d messages from disk from offset %d", s, len(messages), fromOffset)
	for _, m := range messages {
		handleMessage(s, m, handler)
	}

	var lastOffset *uint64
	if len(messages) != 0 {
		lastOffset = &messages[len(messages)-1].Offset
		log.Debugf("%s last message offset from disk %v", s, *lastOffset)
	} else {
		log.Debugf("%sLast message offset is nil", s)
	}

	for {
		select {
		case msg := <-s.gate.MsgChan:
			log.Tracef("%s received %s", s, msg)
			if lastOffset == nil || msg.Offset > *lastOffset {
				handleMessage(s, msg, handler)
			} else {
				log.Debugf("Skipping message %s", msg)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// Sends message and increments the offset of subscriber
// At least once semantic
func handleMessage(s *Subscriber, message *msgservice.Message, handler func([]byte) error) {
	err := handler(message.Body)
	if err == nil {
		err := offsetrepo.SubscriberOffsetStorage.Update(&offsetrepo.SubscriberGroup{Topic: s.Topic, Group: s.Group}, message.Offset)
		if err != nil {
			log.Errorf("Couldn't increment offset: %s", err.Error())
		}
	}
}

func (s *Subscriber) Close() {
	if s != nil {
		log.Debugf("Lost subscriber on Topic %s", s)
		s.gate.Close()
	}
}
