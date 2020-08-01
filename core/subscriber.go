package core

import (
	"context"
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/core/domain"
	"github.com/gl-ot/light-mq/core/gate"
	"github.com/gl-ot/light-mq/core/offset/offsetrepo"
	"github.com/gl-ot/light-mq/core/record/lmqlog"
	"github.com/gl-ot/light-mq/core/recordlb"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type Subscriber struct {
	sub *domain.Subscriber
	partitionIds []int
}

func (s Subscriber) String() string {
	return s.sub.String()
}

func (s Subscriber) toPartitions() []domain.Partition {
	var partitions []domain.Partition
	for _, p := range s.partitionIds {
		partitions = append(partitions, domain.Partition{ID: p, Topic: s.sub.Topic})
	}
	return partitions
}

func NewSub(topic string, group string) (*Subscriber, error) {
	if topic == "" {
		return nil, emptyTopicError
	}
	if group == "" {
		return nil, InputError{Msg: "group can't be empty"}
	}

	err := config.MkDirGroup(topic, group)
	if err != nil {
		return nil, err
	}

	subId := domain.SubscriberID(uuid.New().String())
	s := &Subscriber{
		sub: &domain.Subscriber{ID: subId, SGroup: domain.SGroup{Topic: topic, Group: group}},
	}

	log.Debugf("New %s", s)

	return s, nil
}

// Invokes handler on every new message.
// Blocks until context is canceled.
func (s *Subscriber) Subscribe(ctx context.Context, handler func([]byte) error) error {
	rChan, ids, err := recordlb.StreamRecords(s.sub)
	if err != nil {
		return err
	}
	defer s.close()
	s.partitionIds = ids
	for {
		select {
		case msg := <-rChan:
			handleMessage(s, msg, handler)
		case <-ctx.Done():
			return nil
		}
	}

}

// Sends message and increments the offset of subscriber
// At least once semantic
func handleMessage(s *Subscriber, r *lmqlog.Record, handler func([]byte) error) {
	log.Tracef("%s handling %s", s, r)
	err := handler(r.Body)
	if err == nil {
		err := offsetrepo.SOffset.Update(domain.SGroupPartition{SGroup: s.sub.SGroup, PartitionID: r.PartitionID}, r.Offset)
		if err != nil {
			log.Errorf("Couldn't increment offset: %s", err.Error())
		}
	}
}

func (s *Subscriber) close() {
	if s != nil {
		log.Debugf("Lost subscriber on Topic %s", s)
		for _, p := range s.toPartitions() {
			gate.Close(p, s.sub.ID)
		}
		recordlb.FinishStreamRecord(s.sub)
	}
}
