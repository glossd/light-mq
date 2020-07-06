package offsetStorage

import (
	"encoding/binary"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/gl-ot/light-mq/config"
	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"time"
)

type boltStorage struct {
	db *bolt.DB
}

func createBoltDb() *bolt.DB {
	subsDir := filepath.Join(config.Props.Log.Dir, "subscribers")
	err := os.MkdirAll(subsDir, 0700)
	if err != nil {
		log.Fatalf("Couldn't create directory for subscribers: %s", err.Error())
	}
	pathToDb := filepath.Join(subsDir, "offsets.db")
	// todo close the db connection
	db, err := bolt.Open(pathToDb, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func (b boltStorage) GetLatest(key *SubscriberGroup) (int, error) {
	var offset int64
	var notfound bool
	err := b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(key.Topic))
		if b == nil {
			notfound = true
			return nil
		}
		v := b.Get([]byte(key.Group))
		if v == nil {
			notfound = true
			return nil
		}
		offset = int64(binary.LittleEndian.Uint64(v))
		return nil
	})
	if err != nil {
		log.Errorf("Couldn't get latest offset: %s", err.Error())
		return 0, err
	}
	if notfound {
		return 0, SubscriberGroupNotFound{key}
	}
	return int(offset), nil
}

func (b boltStorage) Store(key *SubscriberGroup, value int) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(key.Topic))
		if err != nil {
			log.Errorf("Couldn't create bucket: %s", err.Error())
			return fmt.Errorf("create bucket: %s", err)
		}
		bvalue := make([]byte, 8)
		binary.LittleEndian.PutUint64(bvalue, uint64(value))
		err = b.Put([]byte(key.Group), bvalue)
		if err != nil {
			log.Errorf("Couldn't save offset: topic: %s, group: %s, err: %s", key.Topic, key.Group, err.Error())
		}
		return err
	})
}
