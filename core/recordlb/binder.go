package recordlb

import (
	"github.com/gl-ot/light-mq/core/domain"
)

// Binds partition ID and subscriber ID.
type Binder struct {
	partId int
	subId domain.SubscriberID
}
