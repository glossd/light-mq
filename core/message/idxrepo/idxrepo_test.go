package idxrepo

import (
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	topic   = "my-topic"
	message = "sup bud?"
)

func TestGetAllFrom(t *testing.T) {
	err := testutil.LogSetup("idxrepo")
	if err != nil {
		t.Fatal(err)
	}
	err = config.MkDirTopic(topic)
	assert.Nil(t, err)

	repo := &dumbMessageIndex{index: make(map[string][]Position)}
	offset, err := repo.Save(topic, []byte(message))
	assert.Nil(t, err)
	assert.Equal(t, 0, offset)

	offset, err = repo.Save(topic, []byte(message))
	assert.Nil(t, err)
	assert.Equal(t, 1, offset)

	positions := repo.GetAllFrom(topic, 0)
	assert.Len(t, positions, 2)
	msgLen := len([]byte(message))
	assert.Equal(t, positions[0], Position{Start: 0, Size: msgLen})
	assert.Equal(t, positions[1], Position{Start: msgLen, Size: msgLen})
}

func TestGetAllFromEmpty(t *testing.T) {
	err := testutil.LogSetup("idxrepo")
	if err != nil {
		t.Fatal(err)
	}
	err = config.MkDirTopic(topic)
	assert.Nil(t, err)

	repo := &dumbMessageIndex{index: make(map[string][]Position)}
	positions := repo.GetAllFrom(topic, 0)
	assert.Len(t, positions, 0)
}

func TestFillUpOnStartUp(t *testing.T) {
	err := testutil.LogSetup("idxrepo")
	if err != nil {
		t.Fatal(err)
	}
	err = config.MkDirTopic(topic)
	assert.Nil(t, err)

	repo := &dumbMessageIndex{index: make(map[string][]Position)}
	firstMsg := []byte(message + "_1")
	offset, err := repo.Save(topic, firstMsg)
	assert.Nil(t, err)
	assert.Equal(t, 0, offset)

	secondMsg := []byte(message + "_2")
	offset, err = repo.Save(topic, secondMsg)
	assert.Nil(t, err)
	assert.Equal(t, 1, offset)

	repo2 := &dumbMessageIndex{index: make(map[string][]Position)}
	repo2.fillInMemoryIndexOnStartUp()
	messages := repo2.GetAllFrom(topic, 0)
	assert.Len(t, messages, 2)
	assert.Equal(t, Position{Start: 0, Size: len(firstMsg)}, messages[0])
	assert.Equal(t, Position{Start: len(firstMsg), Size: len(secondMsg)}, messages[1])
}
