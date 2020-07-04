package indexStorage

import log "github.com/sirupsen/logrus"

type MessageIndex interface {
	SaveMessage(topic string, newMessage []byte) error
	GetAllPositionsFrom(topic string, offset int) []Position
}

type Position struct {
	Start int
	Size  int
}

var TopicMessageIndex MessageIndex

func init() {
	d := &dumbMessageIndex{index: make(map[string][]Position)}
	err := d.fillInMemoryIndexOnStartUp()
	if err != nil {
		log.Fatalf("Couldn't load message index into memory: %s", err.Error())
	}
	TopicMessageIndex = d
}
