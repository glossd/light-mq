package idxrepo

import log "github.com/sirupsen/logrus"

type MessageIndex interface {
	Save(topic string, newMessage []byte) (int, error)
	GetAllFrom(topic string, offset int) []Position
}

type Position struct {
	Start int
	Size  int
}

var TopicMessageIndex MessageIndex

func init() {
	InitIndex()
}

func InitIndex() {
	d := &dumbMessageIndex{index: make(map[string][]Position)}
	err := d.fillInMemoryIndexOnStartUp()
	if err != nil {
		log.Fatalf("Couldn't load message index into memory: %s", err.Error())
	}
	TopicMessageIndex = d
}
