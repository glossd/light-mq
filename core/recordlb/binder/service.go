package binder

import "github.com/gl-ot/light-mq/core/record/lmqlog"

func GetBinders(topic string) []*Binder {
	partitions := lmqlog.GetPartitions(topic)
	var binders []*Binder
	for _, p := range partitions {
		binders = append(binders, New(p.Topic, p.ID))
	}
	return binders
}
