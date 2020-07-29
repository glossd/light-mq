package recordlb

import (
	"github.com/gl-ot/light-mq/core/domain"
	"github.com/gl-ot/light-mq/core/offset/offsetrepo"
	"github.com/gl-ot/light-mq/core/record/lmqlog"
	"github.com/gl-ot/light-mq/core/record/recordstore"
	log "github.com/sirupsen/logrus"
)

var sgroupBinders map[domain.SGroup]*Assigner

func init() {
	Init()
}

func Init() {
	sgroupBinders = make(map[domain.SGroup]*Assigner)
}

func StreamRecords(s *domain.Subscriber) (<-chan *lmqlog.Record, error) {
	sgroup := s.SGroup

	assigner := sgroupBinders[sgroup]
	if assigner == nil {
		assigner = &Assigner{sgroup: &sgroup, binders: []*Binder{}}
		sgroupBinders[sgroup] = assigner
	}

	// todo implement partition assignment
	assigner.addSubscriber(s)
	rChan := make(chan *lmqlog.Record)
	for _, b := range assigner.binders {
		rChanPartition, err := recordstore.GetAllFrom(sgroup.Topic, b.partId, getGroupOffsetOrZero(sgroup, b.partId))
		if err != nil {
			return nil, err
		}
		go func() {
			for r := range rChanPartition {
				rChan <- r
			}
			close(rChan)
		}()
	}

	return rChan, nil
}

func getGroupOffsetOrZero(sgroup domain.SGroup, partitionId int) uint64 {
	o := offsetrepo.SOffset.Get(domain.SGroupPartition{SGroup: sgroup, PartitionID: partitionId})
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
	subId := s.ID
	sgroup := s.SGroup
	assigner := sgroupBinders[sgroup]
	binders := assigner.binders
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
		assigner.binders = binders[:len(binders) - 1]
	}
}
