package index

import (
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/core/record/lmqlog"
	"github.com/gl-ot/light-mq/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	topic = "my-topic"
)

var (
	message = "hello"
	size = uint32(len([]byte(message)))
	firstPosition  = &Position{Start: 0, Size: size}
	secondPosition = &Position{Start: int64(lmqlog.GetRecordSize([]byte(message))), Size: size}
)

func TestGetAllFrom(t *testing.T) {
	setup(t)
	repo := createIndex()

	repo.SaveIntoMemory(topic, firstPosition)
	repo.SaveIntoMemory(topic, secondPosition)

	position := repo.Get(topic, 0)
	position2 := repo.GetLast(topic)
	assert.Equal(t, *firstPosition, position)
	assert.Equal(t, *secondPosition, position2)
}

func TestGetAllFromEmpty(t *testing.T) {
	setup(t)
	repo := createIndex()
	position := repo.Get(topic, 0)
	assert.Equal(t, position, Position{})
}

func TestFillUpOnStartUpWithDump(t *testing.T) {
	setup(t)
	repo := createIndex()

	repo.SaveIntoMemory(topic, firstPosition)
	repo.SaveIntoMemory(topic, secondPosition)

	repo.dumpIndexOnDisk()

	repo2 := createIndex()
	repo2.fillIndex()

	assert.Equal(t, *firstPosition, repo.Get(topic, 0))
	assert.Equal(t, *secondPosition, repo.GetLast(topic))
}

func TestFillUpOnStartUpFromLog(t *testing.T) {
	setup(t)

	lmqlog.InitLogStorage()
	_, err := lmqlog.Log.Store(topic, []byte(message))
	assert.Nil(t, err)
	_, err = lmqlog.Log.Store(topic, []byte(message))
	assert.Nil(t, err)

	InitIndex()

	assert.Equal(t, *firstPosition, Index.Get(topic, 0))
	assert.Equal(t, *secondPosition, Index.GetLast(topic))
}

func setup(t *testing.T) {
	err := testutil.LogSetup("index")
	if err != nil {
		t.Fatal(err)
	}
	err = config.MkDirTopic(topic)
	assert.Nil(t, err)
}

func createIndex() *RecordIndex {
	return &RecordIndex{index: make(map[string][]*Position), lastStoredOffsets: make(map[string]uint64)}
}
