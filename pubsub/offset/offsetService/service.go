package offsetService

import (
	"github.com/gl-ot/light-mq/pubsub/offset/offsetStorage"
)

/*
	If offset doesn't exist it saves zero offset and returns it
*/
func ComputeSubscriberOffset(sg *offsetStorage.SubscriberGroup) (int, error) {
	offset, err := offsetStorage.SubscriberOffsetStorage.GetLatest(sg)
	if _, ok := err.(offsetStorage.SubscriberGroupNotFound); ok {
		err := offsetStorage.SubscriberOffsetStorage.Store(sg, 0)
		if err != nil {
			return 0, err
		}
		return 0, nil

	} else if err != nil {
		return 0, err
	}
	return offset, nil
}

/*
	Returns incremented offset.
	todo it's not thread safe
*/
func IncrementOffset(sg *offsetStorage.SubscriberGroup) (int, error) {
	// todo create zero offset???
	latest, err := offsetStorage.SubscriberOffsetStorage.GetLatest(sg)
	// todo maybe GetLatest doesn't need to return error
	if err != nil {
		return 0, err
	}
	newLatestOffset := latest + 1
	err = offsetStorage.SubscriberOffsetStorage.Store(sg, newLatestOffset)
	if err != nil {
		return 0, err
	}
	return newLatestOffset, nil
}
