package gates

import (
	"github.com/gl-ot/light-mq/core/message/msgservice"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"sync"
)

type Gate struct {
	UUID     string
	Topic    string
	SubGroup string
	MsgChan  chan *msgservice.Message
}

var topicsWithGates = sync.Map{}

// For publishers
func SendMessage(topic string, message *msgservice.Message) {
	gates, ok := topicsWithGates.Load(topic)
	if ok {
		gates.(*sync.Map).Range(func(k interface{}, v interface{}) bool {
			v.(*Gate).MsgChan <- message
			return true
		})
	}
}

func New(topic, group string) *Gate {
	return &Gate{
		UUID:     uuid.New().String(),
		Topic:    topic,
		SubGroup: group,
		MsgChan:  make(chan *msgservice.Message, 16),
	}
}

func (g *Gate) Open() {
	gates, _ := topicsWithGates.LoadOrStore(g.Topic, &sync.Map{})
	gates.(*sync.Map).Store(g.UUID, g)
}

func (g *Gate) Close() {
	gates, ok := topicsWithGates.Load(g.Topic)
	if ok {
		gates.(*sync.Map).Delete(g.UUID)
		close(g.MsgChan)
	} else {
		log.Warnf("Didn't find any gates in topic=%s", g.Topic)
	}
}
