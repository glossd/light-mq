package index

import (
	"encoding/binary"
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/core/record/lmqlog"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

// Stores record positions in memory for the fast lookup.
// Scheduled to save the index on disk every log.index.dumpSec.
// On startup it restores record positions from the disk and then
// reads the log for missing on the disk positions to add the up.
type RecordIndex struct {
	index             map[string][]*Position
	lastStoredOffsets map[string]uint64
}

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
		records, err := lmqlog.Log.GetAllFrom(topic, d.GetLast(topic).Start)
		if err != nil {
			log.Fatalf("Couldn't get records: %s", err)
		}
		for r := range records {
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

// If topic is empty then returns empty Position{Start:0,Size:0}
func (d *RecordIndex) Get(topic string, offset uint64) Position {
	positions, ok := d.index[topic]
	if !ok {
		return Position{}
	}
	if uint64(len(positions)) < offset {
		log.Fatalf("Possible data corruption! "+
			"Last index offset less then requested one: topic=%s, lastIndexOffset=%d, offset=%d",
			topic, len(positions), offset)
	}
	return *positions[offset]
}

func (d *RecordIndex) GetLast(topic string) Position {
	length := len(d.index[topic])
	if length == 0 {
		return Position{}
	}
	return d.Get(topic, uint64(length-1))
}

func (d *RecordIndex) SaveIntoMemory(topic string, position *Position) {
	d.index[topic] = append(d.index[topic], position)
}

func (d *RecordIndex) storeOnFileSystem(topic string, position *Position) error {
	indexPath := filepath.Join(config.TopicDir(topic), "0.idx")
	indexFile, err := os.OpenFile(indexPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	buf := make([]byte, 12)
	binary.LittleEndian.PutUint64(buf, uint64(position.Start))
	binary.LittleEndian.PutUint32(buf[8:], position.Size)
	_, err = indexFile.Write(buf)
	return err
}

func (d *RecordIndex) fillIndex() error {
	dir, err := ioutil.ReadDir(config.TopicsDir())
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, topicDirInfo := range dir {
		topic := topicDirInfo.Name()
		indexPath := filepath.Join(config.TopicDir(topic), "0.idx")
		indexContent, err := ioutil.ReadFile(indexPath)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}

		var positions []*Position
		buf := make([]byte, 12)
		i := 0
		var position Position
		for _, v := range indexContent {
			buf[i] = v
			if i == 11 {
				position.Size = binary.LittleEndian.Uint32(buf[8:])
				positions = append(positions, &Position{Start: position.Start, Size: position.Size})
				i = 0
			} else if i == 7 {
				position.Start = int64(binary.LittleEndian.Uint64(buf[:8]))
				i++
			} else {
				i++
			}
		}

		d.index[topic] = positions
		d.lastStoredOffsets[topic] = uint64(len(positions) - 1)
	}
	return nil
}

func (d *RecordIndex) dumpIndexOnDisk() {
	for topic, positions := range d.index {
		for _, p := range positions {
			err := d.storeOnFileSystem(topic, p)
			if err != nil {
				log.Errorf("Couldn't store record index on disk: topic=%s, position=%d", topic, p.Start)
			}
			d.lastStoredOffsets[topic]++
		}
	}
}
