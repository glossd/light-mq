package lockutil

import (
	log "github.com/sirupsen/logrus"
	"sync"
)

type TopicLock struct {
	topicLocks sync.Map
}

func NewTopicLock() *TopicLock {
	return &TopicLock{}
}

func (tl *TopicLock) Lock(topic string) {
	mutex, _ := tl.topicLocks.LoadOrStore(topic, &sync.Mutex{})
	mutex.(*sync.Mutex).Lock()
}
func (tl *TopicLock) Unlock(topic string) {
	mutex, ok := tl.topicLocks.Load(topic)
	if ok {
		mutex.(*sync.Mutex).Unlock()
	} else {
		log.Warnf("Topic Lock couldn't be unlocked, because lock on topic didn't exist: topic=%s", topic)
	}
}


