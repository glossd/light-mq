package subchan

import (
	"github.com/gl-ot/light-mq/core/domain"
	"github.com/gl-ot/light-mq/core/record/lmqlog"
	"sync"
)

// Holds record channels for each subscriber.
// Subscriber can be assigned to many partitions
// therefore subscriber record channel consumes records
// from many partition record channels.
var subChannels sync.Map // [domain.SubscriberID]chan *lmqlog.Record

func init() {
	Init()
}

func Init() {
	subChannels = sync.Map{}
}

func New(subId domain.SubscriberID) chan *lmqlog.Record {
	rChan, _ := subChannels.LoadOrStore(subId, make(chan *lmqlog.Record))
	return rChan.(chan *lmqlog.Record)
}
