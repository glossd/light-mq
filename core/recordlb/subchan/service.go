package subchan

import (
	"github.com/gl-ot/light-mq/core/domain"
	"github.com/gl-ot/light-mq/core/record/lmqlog"
	"sync"
)

// Subscriber can have many partitions.
// Partition can have many groups, but
// only one subscriber in that group

// todo thread safety

type Subscriber struct {
	ID domain.SubscriberID
	RChan chan *lmqlog.Record
}

func NewSubscriber(subId domain.SubscriberID) *Subscriber {
	rChan, _ := subChannels.LoadOrStore(subId, make(chan *lmqlog.Record))
	return &Subscriber{ID: subId, RChan: rChan.(chan *lmqlog.Record)}
}

func (s *Subscriber) AddPartition(topic string, partitionId int) {
	partSub[key{topic: topic, partitionId: partitionId}] = s.ID
}

type key struct {
	topic       string
	partitionId int
}

var partSub map[key]domain.SubscriberID
// map[domain.SubscriberID]chan *lmqlog.Record
var subChannels sync.Map

func init() {
	Init()
}

func Init() {
	partSub = make(map[key]domain.SubscriberID)
	subChannels = sync.Map{}
}
