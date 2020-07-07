package idxrepo

import (
	"fmt"
	"github.com/gl-ot/light-mq/config"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type dumbMessageIndex struct {
	index map[string][]Position
}

func (d *dumbMessageIndex) GetAllPositionsFrom(topic string, offset int) []Position {
	return d.index[topic][offset:]
}

func (d *dumbMessageIndex) SaveMessage(topic string, newMessage []byte) (int, error) {
	var newStart int
	if len(d.index[topic]) == 0 {
		newStart = 0
	} else {
		prevMsgPos := d.index[topic][len(d.index)-1]
		newStart = prevMsgPos.Start + prevMsgPos.Size
	}

	size := len(newMessage)
	err := d.storeOnFileSystem(topic, newStart, size)
	if err != nil {
		return 0, err
	}

	newPosition := Position{Start: newStart, Size: size}
	d.index[topic] = append(d.index[topic],
		newPosition)

	return len(d.index[topic]) - 1, nil
}

func (d *dumbMessageIndex) storeOnFileSystem(topic string, start, size int) error {
	indexPath := filepath.Join(config.TopicDir(topic), "0.idx")
	indexFile, err := os.OpenFile(indexPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = indexFile.WriteString(fmt.Sprintf("%d,%d\n", start, size))
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
		indexPath := filepath.Join(config.TopicDir(topicDirInfo.Name()), "0.idx")
		indexContent, err := ioutil.ReadFile(indexPath)
		if err != nil {
			return err
		}
		positionStrings := strings.Split(string(indexContent), "\n")
		positionStrings = positionStrings[:len(positionStrings)-1] // last split is empty
		var positions []Position
		for _, p := range positionStrings {
			b := strings.Split(p, ",")
			start, _ := strconv.Atoi(b[0])
			size, _ := strconv.Atoi(b[1])
			newPosition := Position{Start: start, Size: size}
			positions = append(positions, newPosition)
		}

		d.index[topicDirInfo.Name()] = positions
	}
	return nil
}
