package index

import (
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	topic = "my-topic"
)

var (
	firstPosition  = &Position{Start: 0, Size: 30}
	secondPosition = &Position{Start: 38, Size: 30}
)

func TestGetAllFrom(t *testing.T) {
	setup(t)

	repo := createIndex()

	repo.SaveIntoMemory(topic, firstPosition)
	repo.SaveIntoMemory(topic, secondPosition)

	positions := repo.GetAllFrom(topic, 0)
	assert.Len(t, positions, 2)
	assert.Equal(t, *positions[0], *firstPosition)
	assert.Equal(t, *positions[1], *secondPosition)
}

func TestGetAllFromEmpty(t *testing.T) {
	setup(t)

	repo := createIndex()
	positions := repo.GetAllFrom(topic, 0)
	assert.Len(t, positions, 0)
}

func TestFillUpOnStartUp(t *testing.T) {
	setup(t)

	repo := createIndex()

	repo.SaveIntoMemory(topic, firstPosition)
	repo.SaveIntoMemory(topic, secondPosition)

	repo.dumpIndexOnDisk()

	repo2 := createIndex()
	repo2.fillIndex()
	positions := repo2.GetAllFrom(topic, 0)
	assert.Len(t, positions, 2)
	assert.Equal(t, *firstPosition, *positions[0])
	assert.Equal(t, *secondPosition, *positions[1])
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
