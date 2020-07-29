package recordlb

import (
	"github.com/gl-ot/light-mq/core/domain"
	"github.com/gl-ot/light-mq/core/record/lmqlog"
	"sync/atomic"
)

// Partitions are assigned with round robin.
// Number of binders are always equal to number of partitions.
type Assigner struct {
	sgroup *domain.SGroup
	binders []*Binder
	subscribers []*domain.Subscriber
	nextIdx uint32
}

func (a *Assigner) addSubscriber(s *domain.Subscriber) {
	a.subscribers = append(a.subscribers, s)
	if a.isBindersFilledUp() {
		lmqlog.CreatePartition(s.Topic)
	}
	a.reassignPartitions()
}

func (a *Assigner) removeSubscriber(s *domain.Subscriber) {
	for i, v := range a.subscribers {
		if v.ID == s.ID {
			a.subscribers = append(a.subscribers[:i], a.subscribers[i + 1:]...)
			break
		}
	}
	a.reassignPartitions()
}

// Each partition has its own subscriber
func (a *Assigner) isBindersFilledUp() bool {
	setOfSubs := make(map[domain.SubscriberID]struct{})
	for _, b := range a.binders {
		setOfSubs[b.subId] = struct{}{}
	}

	return len(setOfSubs) > len(a.binders)
}

func (a *Assigner) reassignPartitions() {
	partitions := lmqlog.GetPartitions(a.sgroup.Topic)
	var binders []*Binder
	for _, p := range partitions {
		binders = append(binders, &Binder{partId: p.ID, subId: a.nextSub().ID})
	}
	a.binders = binders
}

func (a *Assigner) nextSub() *domain.Subscriber {
	n := atomic.AddUint32(&a.nextIdx, 1)
	return a.subscribers[(int(n) - 1) % len(a.subscribers)]
}
