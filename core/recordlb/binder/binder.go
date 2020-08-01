package binder

import (
	"github.com/gl-ot/light-mq/core/domain"
)

// Binds partition ID and subscriber ID.
type Binder struct {
	partition domain.Partition
	subId *domain.SubscriberID
}

func New(topic string, partId int) *Binder {
	return &Binder{partition: domain.Partition{Topic: topic, ID: partId}}
}

func (b *Binder) GetPartitionId() int {
	return b.partition.ID
}

func (b *Binder) GetPartitionTopic() string {
	return b.partition.Topic
}

func (b *Binder) GetSubscriberId() *domain.SubscriberID {
	return b.subId
}

func (b *Binder) SetSubscriberId(subId *domain.SubscriberID) {
	b.subId = subId
}
