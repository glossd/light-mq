package lockutil

import (
	"github.com/gl-ot/light-mq/core/domain"
	log "github.com/sirupsen/logrus"
	"sync"
)

type PartitionLock struct {
	partitionLocks sync.Map
}

func NewPartitionLock() *PartitionLock {
	return &PartitionLock{}
}

func (pl *PartitionLock) Lock(partition domain.Partition) {
	mutex, _ := pl.partitionLocks.LoadOrStore(partition, &sync.Mutex{})
	mutex.(*sync.Mutex).Lock()
}
func (pl *PartitionLock) Unlock(partition domain.Partition) {
	mutex, ok := pl.partitionLocks.Load(partition)
	if ok {
		mutex.(*sync.Mutex).Unlock()
		pl.partitionLocks.Delete(partition)
	} else {
		log.Warnf("Partition Lock couldn't be unlocked, because lock didn't exist: partition=%s", partition)
	}
}
