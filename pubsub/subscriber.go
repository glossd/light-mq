package pubsub

import (
	"context"
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/pubsub/gate"
	"github.com/gl-ot/light-mq/pubsub/message/msgrepo"
	"github.com/gl-ot/light-mq/pubsub/offset/offsetService"
	"github.com/gl-ot/light-mq/pubsub/offset/offsetStorage"
	log "github.com/sirupsen/logrus"
)

func NewSub(topic string, group string) (*Subscriber, error) {
	if topic == "" {
		return nil, emptyTopicError
	}
	if group == "" {
		return nil, InputError{Msg: "Group can't be empty"}
	}

	if err := config.MkDirTopic(topic); err != nil {
		return nil, err
	}

	return &Subscriber{
		Topic: topic,
		Group: group,
	}, nil
}

// Invokes handler on every new message.
// Blocks until context is canceled.
func (s *Subscriber) Subscribe(ctx context.Context, handler func([]byte) error) error {
	// todo probably race condition on two subscribers with the same Group
	offset, err := offsetService.ComputeSubscriberOffset(&offsetStorage.SubscriberGroup{Topic: s.Topic, Group: s.Group})
	if err != nil {
		return err
	}

	stream.Open(s.Topic, s.Group)

	messages, err := msgrepo.GetFrom(s.Topic, offset)
	if err != nil {
		return err
	}
	for _, m := range messages {
		handleMessage(s, m, handler)
	}

	lastOffset := messages[len(messages)-1].Offset

	msgChan := stream.GetMessageChannel(s.Topic, s.Group)

	for {
		select {
		case msg := <-msgChan:
			if msg.Offset > lastOffset {
				handleMessage(s, msg, handler)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// Sends message and increments the offset of subscriber
// At least once semantic
func handleMessage(s *Subscriber, message *msgrepo.Message, handler func([]byte) error) {
	err := handler(message.Body)
	if err == nil {
		// maybe just store offset of message???
		offset, err := offsetService.IncrementOffset(&offsetStorage.SubscriberGroup{Topic: s.Topic, Group: s.Group})
		if offset != message.Offset {
			log.Error("Message offset doesn't correspond to incremented offset of consumer")
		}

		if err != nil {
			log.Errorf("Couldn't increment offset: %s", err.Error())
		}
	}
}

func (s *Subscriber) Close() {
	if s != nil {
		log.Debugf("Lost subscriber on Topic %s", s.Topic)
		stream.Close(s.Topic, s.Group)
	}
}
