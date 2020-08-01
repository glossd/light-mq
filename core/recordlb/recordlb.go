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

// To each subscriber group (domain.SGroup) belongs unique assigner (assigner.Assigner)
var assigners sync.Map // [domain.SGroup]*assigner.Assigner

func init() {
	Init()
}

func Init() {
	assigners = sync.Map{}
}

// Returns subscriber channel and partitionIds
func StreamRecords(s *domain.Subscriber) (<-chan *lmqlog.Record, []int,  error) {
	a := computeAssigner(s)
	subChan := subchan.New(s.ID)
	for _, partitionId := range a.GetSubPartIds(s) {
		partitionChan, err := recordstore.GetAllFrom(s.Topic, partitionId, getGroupOffsetOrZero(s, partitionId))
		if err != nil {
			return nil, nil, err
		}
		go stream(s, partitionId, partitionChan, subChan)
	}

	return subChan, a.GetPartitionIds(), nil
}

func stream(s *domain.Subscriber, partitionId int, partitionChan <-chan *lmqlog.Record, subChan chan *lmqlog.Record) {
	p := domain.Partition{ID: partitionId, Topic: s.Topic}
	// todo open gate right before last record
	lastOffset:= streamFromPartToSub(s, p, partitionChan, subChan)

	streamFromGateToSub(s, p, lastOffset, subChan)
}

// Ends blocking when all records are read from disk partition.
// Returns partition last record offset.
func streamFromPartToSub(s *domain.Subscriber, p domain.Partition,
	partitionChan <-chan *lmqlog.Record, subChan chan *lmqlog.Record) *uint64 {
	gate.Open(p, s.ID)
	var lastRecord *lmqlog.Record
	for r := range partitionChan {
		subChan <- r
		lastRecord = r
	}

	return extractOffset(lastRecord, s)
}

// Blocks until the gate is closed.
func streamFromGateToSub(s *domain.Subscriber, p domain.Partition, lastOffset *uint64, subChan chan *lmqlog.Record) {
	streamingRecords := gate.StreamingRecords(p, s.ID)
	for r := range streamingRecords {
		log.Tracef("%s received %s", s, r)
		if lastOffset == nil || r.Offset > *lastOffset {
			subChan <- r
		} else {
			log.Debugf("%s skipping message %s", s, r)
		}
	}
}

func extractOffset(lastRecord *lmqlog.Record, s *domain.Subscriber) *uint64 {
	var lastOffset *uint64
	if lastRecord != nil {
		lastOffset = &lastRecord.Offset
		log.Debugf("%s last message offset from disk %v", s, *lastOffset)
	} else {
		log.Debugf("%s last message offset is nil", s)
	}
	return lastOffset
}

func computeAssigner(s *domain.Subscriber) *assigner.Assigner {
	sgroup := s.SGroup
	v, _ := assigners.LoadOrStore(sgroup, assigner.NewAssigner(sgroup))
	a := v.(*assigner.Assigner)
	a.AddSubscriber(s)
	return a
}

func getGroupOffsetOrZero(s *domain.Subscriber, partitionId int) uint64 {
	o := offsetrepo.SOffset.Get(domain.SGroupPartition{SGroup: s.SGroup, PartitionID: partitionId})
	log.Debugf("%s starts reading from offset %d", s, o)
	if o == nil {
		return 0
	} else {
		return *o
	}
}

func FinishStreamRecord(s *domain.Subscriber) {
	v, ok := assigners.Load(s.SGroup)
	if ok {
		a := v.(*assigner.Assigner)
		a.RemoveSubscriber(s)
	} else {
		log.Warnf("Couldn't find assigner by %s", s)
	}
}
