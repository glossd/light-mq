package index

import (
	"encoding/binary"
	"github.com/gl-ot/light-mq/config"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path/filepath"
)

type RecordIndex struct {
	index             map[string][]*Position
	lastStoredOffsets map[string]uint64
}

func (d *RecordIndex) GetAllFrom(topic string, offset int) []*Position {
	return d.index[topic][offset:]
}

// If topic is empty then returns empty Position{Start:0,Size:0}
func (d *RecordIndex) Get(topic string, offset uint64) *Position {
	positions, ok := d.index[topic]
	if !ok {
		return &Position{}
	}
	if uint64(len(positions)) < offset {
		log.Fatalf("Possible data corruption! "+
			"Last index offset less then requested one: topic=%s, lastIndexOffset=%d, offset=%d",
			topic, len(positions), offset)
	}
	return positions[offset]
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
