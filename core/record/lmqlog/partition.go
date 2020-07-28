package lmqlog

import (
	"encoding/binary"
	"fmt"
	"github.com/gl-ot/light-mq/config"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
)

// Stores messages in log file.
// Retrieves structured records of those messages
type Partition struct {
	ID int
	lastOffset  uint64
	latPosition int64
}

func (p *Partition) Store(topic string, message []byte) (*Record, error) {
	f, err := p.openFile(topic)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	size := len(message)
	r := NewRecord(p.lastOffset, p.latPosition, size, message)
	// todo what happens when lmq get killed in the middle of writing bytes???
	_, err = f.Write(r.RecordMetaData.toBytes())
	if err != nil {
		log.Errorf("Couldn't write record metadata to %s: %s", f.Name(), err)
		return nil, fmt.Errorf("couldn't write message: %s", err)
	}
	_, err = f.Write(r.Body)
	if err != nil {
		log.Errorf("Couldn't write record body to %s: %s", f.Name(), err)
		return nil, fmt.Errorf("couldn't write message: %s", err)
	}

	p.lastOffset++
	p.latPosition = p.latPosition + metaDataSize + int64(size)

	return r, nil
}

func (p *Partition) GetAllFrom(topic string, position int64) (<-chan *Record, error) {
	f, err := p.openFile(topic)
	if err != nil {
		f.Close()
		return nil, err
	}

	_, err = f.Seek(position, 0)
	if err != nil {
		f.Close()
		return nil, err
	}

	rChan := make(chan *Record)
	go func() {
		defer f.Close()
		for {
			r, err := readRecord(f)
			if err == io.EOF {
				break
			} else if err != nil {
				// todo retry? return records?
				log.Fatalf("Couldn't read a record from the log: %s", err)
			}
			rChan <- r
		}
		close(rChan)
	}()

	return rChan, nil
}

func (p *Partition) openFile(topic string) (*os.File, error) {
	topicDir := config.TopicDir(topic)
	err := os.MkdirAll(topicDir, os.ModePerm)
	if err != nil {
		log.Error("couldn't make topic directory ", err)
		return nil, err
	}

	logPath := filepath.Join(topicDir, p.fileName())

	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Errorf("Couldn't open file %s: %s", logPath, err)
		return nil, fmt.Errorf("couldn't open file %s: %s", logPath, err)
	}
	return f, nil
}

func (p *Partition) fileName() string {
	return fmt.Sprintf("%d.log", p.ID)
}

func readRecord(f *os.File) (*Record, error) {
	metaBuf := make([]byte, metaDataSize)
	_, err := f.Read(metaBuf)
	if err != nil {
		if err != io.EOF {
			log.Errorf("Couldn't read from file %s: %s", f.Name(), err)
		}
		return nil, err
	}
	bodySize := binary.LittleEndian.Uint32(metaBuf[metaSizeStart:])
	bodyBuf := make([]byte, bodySize)
	_, err = f.Read(bodyBuf)
	if err != nil {

		return nil, err
	}
	recordMetaData, _ := metaFromBytes(metaBuf)
	return &Record{RecordMetaData: recordMetaData, Body: bodyBuf}, nil
}
