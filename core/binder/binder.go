package binder

import (
	"github.com/gl-ot/light-mq/core/domain"
	"github.com/gl-ot/light-mq/core/offset/offsetrepo"
	"github.com/gl-ot/light-mq/core/record/lmqlog"
	"github.com/gl-ot/light-mq/core/record/recordstore"
	log "github.com/sirupsen/logrus"
)

// Binds subscriber ID and partition ID
type Binder struct {
	subId string
	partId int
}

var sgroupBinders map[domain.SGroup][]*Binder


func init() {
	Init()
}

func Init() {
	sgroupBinders = make(map[domain.SGroup][]*Binder)
}

// Returns partitionId
func StreamRecords(s *domain.Subscriber) (<-chan *lmqlog.Record, error) {
	sgroup := s.Group
	binders := sgroupBinders[sgroup]
	// todo implement partition assignment
	if isFilledUp(sgroup, binders) {
		newPartId := lmqlog.CreatePartition(sgroup.Topic)
		sgroupBinders[sgroup] = append(sgroupBinders[sgroup], &Binder{subId: s.UUID, partId: newPartId})
		fromOffset := getGroupOffsetOrZero(sgroup)
		log.Debugf("%s reading records from %d offset", s, fromOffset)
		return recordstore.GetAllFrom(sgroup.Topic, newPartId, fromOffset)
	} else {
		// todo take partition from existing subscriber
		return recordstore.GetAllFrom(sgroup.Topic, 0, getGroupOffsetOrZero(sgroup))
	}
}

func isFilledUp(sgroup domain.SGroup,binders []*Binder) bool {
	partitions := lmqlog.GetPartitions(sgroup.Topic)
	return len(partitions) <= len(binders)
}

func getGroupOffsetOrZero(sgroup domain.SGroup) uint64 {
	o := offsetrepo.SubscriberOffsetStorage.Get(&sgroup)
	if o == nil {
		return 0
	} else {
		return *o
	}
}

func FinishStreamRecord(s *domain.Subscriber) {
	// todo give partition to another subscriber
	deleteBinder(s)
}

func deleteBinder(s *domain.Subscriber) {
	subId := s.UUID
	sgroup := s.Group
	binders := sgroupBinders[sgroup]
	deleteIdx := -1
	for i, b := range binders {
		if b.subId == subId {
			deleteIdx = i
			break
		}
	}
	if deleteIdx == -1 {
		log.Warnf("Couldn't delete binder, binder not found, subscriber=%s", s)
	} else {
		binders[deleteIdx] = binders[len(binders) - 1]
		sgroupBinders[sgroup] = binders[:len(binders) - 1]
	}
}
