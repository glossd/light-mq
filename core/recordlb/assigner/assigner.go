package assigner

import (
	"github.com/gl-ot/light-mq/core/domain"
	"github.com/gl-ot/light-mq/core/record/lmqlog"
	"github.com/gl-ot/light-mq/core/recordlb/binder"
	log "github.com/sirupsen/logrus"
	"sync/atomic"
)

// Responsible for assigning partitions to subscribers in the subscriber group (sgroup).
// Assigner belongs to the subscriber group (sgroup).
// Partitions are assigned with round robin.
// Number of Binders are always equal to number of partitions.
type Assigner struct {
	sgroup      *domain.SGroup
	Binders     []*binder.Binder
	subscribers []*domain.Subscriber
	nextIdx     uint32
}

func NewAssigner(sg domain.SGroup) *Assigner {
	return &Assigner{sgroup: &sg, Binders: binder.GetBinders(sg.Topic)}
}

func (a *Assigner) AddSubscriber(s *domain.Subscriber) {
	a.subscribers = append(a.subscribers, s)
	if a.areBindersFilledUp() {
		partitionId := lmqlog.CreatePartition(s.Topic)
		log.Debugf("Created a new partition %d for %s", partitionId, s.SGroup)
		b := binder.New(s.Topic, partitionId)
		a.Binders = append(a.Binders, b)
	}
	a.reassignPartitions()
}

func (a *Assigner) RemoveSubscriber(s *domain.Subscriber) {
	for i, v := range a.subscribers {
		if v.ID == s.ID {
			a.subscribers = append(a.subscribers[:i], a.subscribers[i+1:]...)
			break
		}
	}
	a.reassignPartitions()
}

func (a *Assigner) GetPartitionIds() []int {
	var partitionIds []int
	for _, b := range a.Binders {
		partitionIds = append(partitionIds, b.GetPartitionId())
	}
	return partitionIds
}

func (a *Assigner) GetSubPartIds(s *domain.Subscriber) []int {
	var partitionIds []int
	for _, b := range a.Binders {
		subId := b.GetSubscriberId()
		if subId != nil && *subId == s.ID {
			partitionIds = append(partitionIds, b.GetPartitionId())
		}
	}
	return partitionIds
}

// Each partition has its own subscriber
func (a *Assigner) areBindersFilledUp() bool {
	// first subscriber, there is at least one partition in the topic
	if len(a.subscribers) == 1 {
		return false
	}

	binderSubs := make(map[*domain.SubscriberID]struct{})
	for _, b := range a.Binders {
		binderSubs[b.GetSubscriberId()] = struct{}{}
	}

	return len(a.subscribers) > len(binderSubs)
}

// Assigns to each partition a subscriber from group.
func (a *Assigner) reassignPartitions() {
	for _, b := range a.Binders {
		nextSub := a.nextSub()
		if nextSub == nil {
			log.Debugf("Deassigning partition %d from subscriber", b.GetPartitionId())
			b.SetSubscriberId(nil)
		} else {
			log.Debugf("Assigning partition %d to %s", b.GetPartitionId(), nextSub)
			b.SetSubscriberId(&nextSub.ID)
		}
	}
	a.nextIdx = 0
}

func (a *Assigner) nextSub() *domain.Subscriber {
	n := atomic.AddUint32(&a.nextIdx, 1)
	if len(a.subscribers) == 0 {
		return nil
	}
	return a.subscribers[(int(n)-1)%len(a.subscribers)]
}
