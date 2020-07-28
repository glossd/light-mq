package core

import (
	"context"
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/core/binder"
	"github.com/gl-ot/light-mq/core/domain"
	"github.com/gl-ot/light-mq/core/gates"
	"github.com/gl-ot/light-mq/core/offset/offsetrepo"
	"github.com/gl-ot/light-mq/core/record/lmqlog"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type Subscriber struct {
	sub *domain.Subscriber
	Gate           *gates.Gate
}

func (s Subscriber) String() string {
	return s.sub.String()
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

	subId := uuid.New().String()
	return &Subscriber{
		sub: &domain.Subscriber{UUID: subId, Group: domain.SGroup{Topic: topic, Group: group}},
		Gate:           gates.New(topic, group),
	}, nil
}

// Invokes handler on every new message.
// Blocks until context is canceled.
func (s *Subscriber) Subscribe(ctx context.Context, handler func([]byte) error) error {
	lastRecord, err := s.messagesFromDisk(handler)
	if err != nil {
		return err
	}

	var lastOffset *uint64
	if lastRecord != nil {
		lastOffset = &lastRecord.Offset
		log.Debugf("%s last message offset from disk %v", s, *lastOffset)
	} else {
		log.Debugf("%sLast message offset is nil", s)
	}

	for {
		select {
		case msg := <-s.Gate.MsgChan:
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

func (s *Subscriber) messagesFromDisk(handler func([]byte) error) (*lmqlog.Record, error) {
	// todo race condition multiple subscribers in one group
	diskRecordChan, err := binder.StreamRecords(s.sub)
	if err != nil {
		return nil, err
	}
	defer binder.FinishStreamRecord(s.sub)

	// todo open gate later right before last messages
	s.Gate.Open()

	var lastRecord *lmqlog.Record
	recordCount := 0
	for r := range diskRecordChan {
		handleMessage(s, r, handler)
		recordCount++
		lastRecord = r
	}
	log.Debugf("%s handled %d records", s, recordCount)

	return lastRecord, nil
}

// Sends message and increments the offset of subscriber
// At least once semantic
func handleMessage(s *Subscriber, r *lmqlog.Record, handler func([]byte) error) {
	log.Tracef("%s handling %s", s, r)
	err := handler(r.Body)
	if err == nil {
		err := offsetrepo.SubscriberOffsetStorage.Update(&s.sub.Group, r.Offset)
		if err != nil {
			log.Errorf("Couldn't increment offset: %s", err.Error())
		}
	}
}

// todo move to defer of Subscribe
func (s *Subscriber) Close() {
	if s != nil {
		log.Debugf("Lost subscriber on Topic %s", s)
		s.Gate.Close()
	}
}
