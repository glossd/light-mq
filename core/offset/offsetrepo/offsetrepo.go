package offsetrepo

import (
	"encoding/binary"
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/core/domain"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
)

type SOffsetRepo struct {
	offsets sync.Map
}

var SOffset *SOffsetRepo

func init() {
	InitStorage()
}

func InitStorage() {
	fs := SOffsetRepo{}
	err := fs.fillOffsetsOnStartUp()
	if err != nil {
		log.Fatal(err)
	}
	SOffset = &fs
}

func (fs *SOffsetRepo) Get(sg domain.SGroupPartition) *uint64 {
	e, _ := fs.offsets.Load(sg)
	if e == nil {
		return nil
	}
	return e.(*uint64)
}

// todo not thread safe
func (fs *SOffsetRepo) Update(sg domain.SGroupPartition, newOffset uint64) error {
	latest := fs.Get(sg)
	if latest != nil {
		if newOffset != *latest+1 {
			log.Fatalf("Data corruption! New group offset doesn't equal incremented latest offset: new=%d, latest=%d",
				newOffset, *latest)
		}
	} else if newOffset != 0 {
		log.Fatalf("Data corruption! First group newOffset doesn't equal zero: new=%d", newOffset)
	}

	fPath := config.GroupFile(sg.Topic, sg.Group, sg.PartitionID)
	f, err := os.OpenFile(fPath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		log.Errorf("Couldn't open file %s: %s", fPath, err)
		return err
	}
	defer f.Close()

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, newOffset)

	_, err = f.WriteAt(buf, 0)
	if err != nil {
		log.Errorf("Couldn't write to file %s: %s", fPath, err)
		return err
	}
	fs.putOffsetIntoMemory(sg, newOffset)
	return nil
}

func (fs *SOffsetRepo) fillOffsetsOnStartUp() error {
	dir, err := ioutil.ReadDir(config.TopicsDir())
	if err != nil {
		log.Errorf("Couldn't read topics dir %s: %s", config.TopicsDir(), err)
		return err
	}
	for _, v := range dir {
		topic := v.Name()
		groupsDir, err := ioutil.ReadDir(config.GroupsDir(topic))
		if err != nil {
			log.Errorf("Couldn't read groups dir %s: %s", config.GroupsDir(topic), err)
			return err
		}

		for _, group := range groupsDir {
			groupName := group.Name()
			partitions, err := ioutil.ReadDir(config.GroupDir(topic, groupName))
			if err != nil {
				log.Errorf("Couldn't read group dir %s: %s", config.GroupDir(topic, groupName), err)
				return err
			}

			for _, partition := range partitions {
				pStr := partition.Name()
				partitionId, err := strconv.Atoi(pStr)
				if err != nil {
					log.Errorf("Partition file for group offset is not a number in %s: %s", config.GroupDir(topic, groupName), err)
					return err
				}
				pPath := config.GroupFile(topic, groupName, partitionId)
				b, err := ioutil.ReadFile(pPath)
				if err != nil {
					log.Errorf("Couldn't read group file %s: %s", pPath, err)
					return err
				}
				offset := binary.LittleEndian.Uint64(b)
				sg := domain.SGroup{Topic: topic, Group: groupName}
				sgp := domain.SGroupPartition{SGroup: sg, PartitionID: partitionId}
				fs.putOffsetIntoMemory(sgp, offset)
			}
		}
	}
	return nil
}

func (fs *SOffsetRepo) putOffsetIntoMemory(sg domain.SGroupPartition, newOffset uint64) {
	fs.offsets.Store(sg, &newOffset)
}
