package pubsub

import (
	"context"
	"github.com/gl-ot/light-mq/pubsub/gate"
	"github.com/gl-ot/light-mq/pubsub/message/msgrepo"
	"github.com/gl-ot/light-mq/pubsub/offset/offsetrepo"
	log "github.com/sirupsen/logrus"
)

func NewSub(topic string, group string) (*Subscriber, error) {
	if topic == "" {
		return nil, emptyTopicError
	}
	if group == "" {
		return nil, InputError{Msg: "Group can't be empty"}
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
	offset, err := offsetrepo.SubscriberOffsetStorage.GetLatest(&offsetrepo.SubscriberGroup{Topic: s.Topic, Group: s.Group})
	if err != nil {
		return err
	}

	gate.Open(s.Topic, s.Group)

	var fromOffset int
	if offset != nil {
		fromOffset = *offset
	}
	messages, err := msgrepo.GetAllFrom(s.Topic, fromOffset)
	if err != nil {
		return err
	}
	for _, m := range messages {
		handleMessage(s, m, handler)
	}

	lastOffset := messages[len(messages)-1].Offset

	msgChan := gate.GetMessageChannel(s.Topic, s.Group)

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
		// todo check if newOffset == latestOffset + 1
		err := offsetrepo.SubscriberOffsetStorage.Store(&offsetrepo.SubscriberGroup{Topic: s.Topic, Group: s.Group}, message.Offset)
		if err != nil {
			log.Errorf("Couldn't increment offset: %s", err.Error())
		}
	}
}

func (s *Subscriber) Close() {
	if s != nil {
		log.Debugf("Lost subscriber on Topic %s", s.Topic)
		gate.Close(s.Topic, s.Group)
	}
}
