package recordlb

import (
	"github.com/gl-ot/light-mq/core/domain"
	"github.com/gl-ot/light-mq/core/offset/offsetrepo"
	"github.com/gl-ot/light-mq/core/record/lmqlog"
	"github.com/gl-ot/light-mq/core/record/recordstore"
	"github.com/gl-ot/light-mq/core/recordlb/assigner"
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

func StreamRecords(s *domain.Subscriber) (<-chan *lmqlog.Record, error) {
	sgroup := s.SGroup

	v, _ := sgroupAssigners.LoadOrStore(sgroup, assigner.NewAssigner(sgroup))
	assigner := v.(*assigner.Assigner)

	assigner.AddSubscriber(s)

	var wg sync.WaitGroup
	rChan := make(chan *lmqlog.Record)
	for _, b := range assigner.Binders {
		wg.Add(1)
		partitionId := b.GetPartitionId()
		rChanPartition, err := recordstore.GetAllFrom(sgroup.Topic, partitionId, getGroupOffsetOrZero(sgroup, partitionId))
		if err != nil {
			return nil, err
		}
		go func() {
			for r := range rChanPartition {
				rChan <- r
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(rChan)
	}()

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
	sgroup := s.SGroup
	v, ok := sgroupAssigners.Load(sgroup)
	if !ok {
		log.Warnf("Couldn't find assigner by %s", s)
		return
	}
	a := v.(*assigner.Assigner)
	a.RemoveSubscriber(s)
}
