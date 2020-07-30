package binder

import (
	"github.com/gl-ot/light-mq/core/domain"
	"github.com/gl-ot/light-mq/core/record/lmqlog"
)

// Binds partition ID and subscriber ID.
type Binder struct {

	partId int
	// todo all records from partition are streamed to rChan
	rChan chan *lmqlog.Record

	subId *domain.SubscriberID
}

func New(partId int) *Binder {
	return &Binder{partId: partId, rChan: make(chan *lmqlog.Record)}
}

func (b *Binder) GetPartitionId() int {
	return b.partId
}

func (b *Binder) GetPartitionChan() chan *lmqlog.Record {
	return b.rChan
}

func (b *Binder) GetSubscriberId() *domain.SubscriberID {
	return b.subId
}

func (b *Binder) SetSubscriberId(subId *domain.SubscriberID) {
	b.subId = subId
}
