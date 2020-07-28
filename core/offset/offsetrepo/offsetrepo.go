package offsetrepo

import (
	"encoding/binary"
	"fmt"
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/core/domain"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

var SubscriberOffsetStorage *SubscriberOffsetFile

func init() {
	InitStorage()
}

func InitStorage() {
	fs := SubscriberOffsetFile{}
	fs.fillOffsetsOnStartUp()
	SubscriberOffsetStorage = &fs
}

type SubscriberOffsetFile struct {
	offsets sync.Map
}

func (fs *SubscriberOffsetFile) getOffset(sg *domain.SGroup) *uint64 {
	e, _ := fs.offsets.Load(sg.AsKey())
	if e == nil {
		return nil
	}
	return e.(*uint64)
}

func (fs *SubscriberOffsetFile) putOffset(sg *domain.SGroup, newOffset uint64) {
	fs.offsets.Store(sg.AsKey(), &newOffset)
}

func (fs *SubscriberOffsetFile) Get(sg *domain.SGroup) *uint64 {
	return fs.getOffset(sg)
}

// todo not thread safe
func (fs *SubscriberOffsetFile) Update(sg *domain.SGroup, newOffset uint64) error {
	latest := fs.getOffset(sg)
	if latest != nil {
		if newOffset != *latest+1 {
			log.Fatalf("Data corruption! New group offset doesn't equal incremented latest offset: new=%d, latest=%d",
				newOffset, *latest)
		}
	} else if newOffset != 0 {
		log.Fatalf("Data corruption! First group newOffset doesn't equal zero: new=%d", newOffset)
	}

	fPath := filepath.Join(config.GroupDir(sg.Topic, sg.Group), "offset")
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
	fs.putOffset(sg, newOffset)
	return nil
}

func (fs *SubscriberOffsetFile) fillOffsetsOnStartUp() error {
	dir, err := ioutil.ReadDir(config.TopicsDir())
	if err != nil {
		log.Errorf("Couldn't read topics dir %s: %s", config.TopicsDir(), err)
		return err
	}
	for _, v := range dir {
		topic := v.Name()
		groupDir, err := ioutil.ReadDir(config.GroupsDir(topic))
		if err != nil {
			log.Errorf("Couldn't read groups dir %s: %s", config.TopicsDir(), err)
			return err
		}

		for _, group := range groupDir {
			group := group.Name()
			groupPath := filepath.Join(config.GroupDir(topic, group), "offset")
			b, err := ioutil.ReadFile(groupPath)
			if err != nil {
				log.Errorf("Couldn't read group file %s: %s", groupPath, err)
				return err
			}
			offset := binary.LittleEndian.Uint64(b)
			sg := &domain.SGroup{Topic: topic, Group: group}
			fs.putOffset(sg, offset)
		}
	}
	return nil
}

type SubscriberGroupNotFound struct {
	*SubscriberGroup
}

func (e SubscriberGroupNotFound) Error() string {
	return fmt.Sprintf("not found group: %s, topic: %s", e.SubscriberGroup.Group, e.SubscriberGroup.Topic)
}
