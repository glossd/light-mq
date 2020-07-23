package index

import (
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/core/message/msgrepo"
	log "github.com/sirupsen/logrus"
	"time"
)

type Position struct {
	Start int64
	Size  uint32
}

var Index *RecordIndex

func init() {
	InitIndex()
}

func InitIndex() {
	d := &RecordIndex{index: make(map[string][]*Position), lastStoredOffsets: make(map[string]uint64)}
	err := d.fillIndex()
	if err != nil {
		log.Fatalf("Couldn't load message index into memory: %s", err.Error())
	}
	topics, err := config.ListTopics()
	if err != nil {
		log.Fatalf("Couldn't initialize index: %s", err)
	}
	for _, topic := range topics {
		records, err := msgrepo.GetAllFrom(topic, d.GetLast(topic).Start)
		if err != nil {
			log.Fatalf("Couldn't get records: %s", err)
		}
		for _, r := range records {
			d.SaveIntoMemory(topic, &Position{Start: r.Position, Size: r.Size})
		}
	}
	go func() {
		t := time.NewTicker(time.Second * time.Duration(config.Props.Log.Index.DumpSec))
		for {
			select {
			case <-t.C:
				d.dumpIndexOnDisk()
			}
		}
	}()
	Index = d
}
