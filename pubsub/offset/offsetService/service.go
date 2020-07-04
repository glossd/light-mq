package offsetService

import (
	"github.com/gl-ot/light-mq/pubsub/offset/offsetStorage"
)

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
