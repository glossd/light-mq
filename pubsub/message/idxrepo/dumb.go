package idxrepo

import (
	"encoding/binary"
	"github.com/gl-ot/light-mq/config"
	"io/ioutil"
	"os"
	"path/filepath"
)

type dumbMessageIndex struct {
	index map[string][]Position
}

func (d *dumbMessageIndex) GetAllFrom(topic string, offset int) []Position {
	return d.index[topic][offset:]
}

func (d *dumbMessageIndex) Save(topic string, newMessage []byte) (int, error) {
	var newStart int
	topicIdx := d.index[topic]
	if len(topicIdx) == 0 {
		newStart = 0
	} else {
		prevMsgPos := topicIdx[len(topicIdx)-1]
		newStart = prevMsgPos.Start + prevMsgPos.Size
	}

	size := len(newMessage)
	err := d.storeOnFileSystem(topic, newStart, size)
	if err != nil {
		return 0, err
	}

	newPosition := Position{Start: newStart, Size: size}
	topicIdx = append(topicIdx,
		newPosition)

	d.index[topic] = topicIdx

	return len(topicIdx) - 1, nil
}

func (d *dumbMessageIndex) storeOnFileSystem(topic string, start, size int) error {
	indexPath := filepath.Join(config.TopicDir(topic), "0.idx")
	indexFile, err := os.OpenFile(indexPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf, uint32(start))
	binary.LittleEndian.PutUint32(buf[4:], uint32(size))
	_, err = indexFile.Write(buf)
	return err
}

func (d *dumbMessageIndex) fillInMemoryIndexOnStartUp() error {
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

		var positions []Position
		buf := make([]byte, 8)
		i := 0
		var position Position
		for _, v := range indexContent {
			buf[i] = v
			if i == 7 {
				position.Size = int(binary.LittleEndian.Uint32(buf[4:]))
				positions = append(positions, position)
				i = 0
			} else if i == 3 {
				position.Start = int(binary.LittleEndian.Uint32(buf[:4]))
				i++
			} else {
				i++
			}
		}

		d.index[topic] = positions
	}
	return nil
}
