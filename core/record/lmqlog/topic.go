package lmqlog

import (
	"fmt"
	"sync"
)

// represents map[string]*PartitionLB
var topicPartitions *sync.Map

func init() {
	InitLogStorage()
}

func InitLogStorage() {
	topicPartitions = &sync.Map{}
}

func Store(topic string, message []byte) (*Record, error) {
	plb := computePartitionLB(topic)
	return plb.next().Store(topic, message)
}

func StreamRecordsFrom(topic string, partitionId int, position int64) (<-chan *Record, error) {
	plb := computePartitionLB(topic)

	if partitionId < 0 || partitionId >= len(plb.partitions) {
		return nil, fmt.Errorf("partitionId %d doesn't exist", partitionId)
	}
	partition := plb.partitions[partitionId]
	return partition.GetAllFrom(topic, position)
}

func CreatePartition(topic string) int {
	lb := computePartitionLB(topic)
	lb.partitions = append(lb.partitions, NewPartition(len(lb.partitions), topic))
	return len(lb.partitions)
}

func GetPartitions(topic string) []*Partition {
	return computePartitionLB(topic).partitions
}

func computePartitionLB(topic string) *PartitionLB {
	// todo if not ok create directory for Topic and partition
	v, _ := topicPartitions.LoadOrStore(topic, &PartitionLB{partitions: []*Partition{NewPartition(0, topic)}})
	return v.(*PartitionLB)
}
