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
type FileLog struct {
	lastOffset  uint64
	latPosition int64
}

var Log *FileLog

func init() {
	InitLogStorage()
}

func InitLogStorage() {
	Log = &FileLog{}
}

func (l *FileLog) Store(topic string, message []byte) (*Record, error) {
	f, err := openFile(topic)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	size := len(message)
	r := NewRecord(l.lastOffset, l.latPosition, size, message)
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

	l.lastOffset++
	l.latPosition = l.latPosition + metaDataSize + int64(size)

	return r, nil
}

func (l *FileLog) GetAllFrom(topic string, position int64) (<-chan *Record, error) {
	f, err := openFile(topic)
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

func openFile(topic string) (*os.File, error) {
	topicDir := config.TopicDir(topic)
	err := os.MkdirAll(topicDir, os.ModePerm)
	if err != nil {
		log.Error("couldn't make topic directory ", err)
		return nil, err
	}

	logPath := filepath.Join(topicDir, "0.log")

	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Errorf("Couldn't open file %s: %s", logPath, err)
		return nil, fmt.Errorf("couldn't open file %s: %s", logPath, err)
	}
	return f, nil
}
