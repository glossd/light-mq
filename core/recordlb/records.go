package recordlb

import (
	"github.com/gl-ot/light-mq/core/domain"
	"github.com/gl-ot/light-mq/core/gate"
	"github.com/gl-ot/light-mq/core/offset/offsetrepo"
	"github.com/gl-ot/light-mq/core/record/lmqlog"
	"github.com/gl-ot/light-mq/core/record/recordstore"
	"github.com/gl-ot/light-mq/core/recordlb/assigner"
	"github.com/gl-ot/light-mq/core/recordlb/subchan"
	log "github.com/sirupsen/logrus"
	"sync"
)

var sgroupAssigners sync.Map

func init() {
	Init()
}

func Init() {
	sgroupAssigners = sync.Map{}
}

// Returns subscriber channel and partitionIds
func StreamRecords(s *domain.Subscriber) (<-chan *lmqlog.Record, []int,  error) {
	sgroup := s.SGroup

	v, _ := sgroupAssigners.LoadOrStore(sgroup, assigner.NewAssigner(sgroup))
	a := v.(*assigner.Assigner)

	a.AddSubscriber(s)
	chanSubscriber := subchan.NewSubscriber(s.ID)
	for _, b := range a.Binders {
		chanSubscriber.AddPartition(b.GetPartitionTopic(), b.GetPartitionId())
	}

	for _, b := range a.Binders {
		partitionId := b.GetPartitionId()
		rChanPartition, err := recordstore.GetAllFrom(sgroup.Topic, partitionId, getGroupOffsetOrZero(sgroup, partitionId))
		if err != nil {
			return nil, nil, err
		}
		go func() {
			p := domain.Partition{ID: partitionId, Topic: sgroup.Topic}
			gate.Open(p, s.ID)
			var lastRecord *lmqlog.Record
			recordCount := 0
			for r := range rChanPartition {
				chanSubscriber.RChan <- r
				recordCount++
				lastRecord = r
			}
			log.Debugf("%s handled %d records", s, recordCount)

			var lastOffset *uint64
			if lastRecord != nil {
				lastOffset = &lastRecord.Offset
				log.Debugf("%s last message offset from disk %v", s, *lastOffset)
			} else {
				log.Debugf("%sLast message offset is nil", s)
			}
			streamingRecords := gate.StreamingRecords(p, s.ID)
			for r := range streamingRecords {
				log.Tracef("%s received %s", s, r)
				if lastOffset == nil || r.Offset > *lastOffset {
					chanSubscriber.RChan <- r
				} else {
					log.Debugf("%s skipping message %s", s, r)
				}
			}
		}()
	}

	var partitionIds []int
	for _, b := range a.Binders {
		partitionIds = append(partitionIds, b.GetPartitionId())
	}

	return chanSubscriber.RChan, partitionIds, nil
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
	sgroup := s.SGroup
	v, ok := sgroupAssigners.Load(sgroup)
	if !ok {
		log.Warnf("Couldn't find assigner by %s", s)
		return
	}
	a := v.(*assigner.Assigner)
	a.RemoveSubscriber(s)
}
