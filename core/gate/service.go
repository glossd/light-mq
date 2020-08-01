package gate

import (
	"github.com/gl-ot/light-mq/core/domain"
	"github.com/gl-ot/light-mq/core/lockutil"
	"github.com/gl-ot/light-mq/core/record/lmqlog"
	log "github.com/sirupsen/logrus"
	"sync"
)

// Just before records are loaded from disk, delivered
// and disk record channel is closed the gate of steaming gets opened.
// Afterwards all incoming records are delivered to subscribers through the gates.

type Subscriber struct {
	id domain.SubscriberID
	rChan chan *lmqlog.Record
}

func NewSub(subId domain.SubscriberID) *Subscriber {
	return &Subscriber{id: subId, rChan: make(chan *lmqlog.Record, 20)}
}

var partitionGates sync.Map // [domain.Partition)][]*Subscriber
var partitionLock *lockutil.PartitionLock

func init() {
	Init()
}

func Init() {
	partitionGates = sync.Map{}
	partitionLock = lockutil.NewPartitionLock()
}

func Open(p domain.Partition, subId domain.SubscriberID) {
	partitionLock.Lock(p)
	defer partitionLock.Unlock(p)
	v, ok := partitionGates.LoadOrStore(p, []*Subscriber{NewSub(subId)})
	if ok {
		subs := v.([]*Subscriber)
		put(p, append(subs, NewSub(subId)))
	}
}

func Close(p domain.Partition, subId domain.SubscriberID) {
	partitionLock.Lock(p)
	defer partitionLock.Unlock(p)
	subs := get(p)
	subIdx := -1
	for i, s := range subs {
		if s.id == subId {
			close(s.rChan)
			subIdx = i
		}
	}
	if subIdx == -1 {
		log.Warnf("Tried to close not existing gate record channel: partition=%v", p)
	} else {
		subs[subIdx] = subs[len(subs) - 1]
		put(p, subs[:len(subs) - 1])
	}
}

// For publishers
func SendRecord(r *lmqlog.Record) {
	subs := get(r.ToPartition())
	for _, s := range subs {
		s.rChan <- r
	}
}

func StreamingRecords(p domain.Partition, subId domain.SubscriberID) chan *lmqlog.Record {
	sub, ok := getSub(p, subId)
	if ok {
		return sub.rChan
	} else {
		return nil
	}
}

func put(p domain.Partition, subs []*Subscriber) {
	partitionGates.Store(p, subs)
}

func getSub(p domain.Partition, subId domain.SubscriberID) (*Subscriber, bool) {
	for _, s := range get(p) {
		if s.id == subId {
			return s, true
		}
	}
	return nil, false
}

func get(p domain.Partition) []*Subscriber {
	v, ok := partitionGates.Load(p)
	if ok {
		return v.([]*Subscriber)
	} else {
		return nil
	}
}
