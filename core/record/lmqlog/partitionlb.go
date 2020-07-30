package lmqlog

import "sync/atomic"

type PartitionLB struct {
	partitions []*Partition
	nextItem uint32
}

func (plb *PartitionLB) next() *Partition {
	n := atomic.AddUint32(&plb.nextItem, 1)
	return plb.partitions[(int(n) - 1) % len(plb.partitions)]
}
