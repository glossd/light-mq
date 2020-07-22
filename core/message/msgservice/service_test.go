package msgservice

import (
	"github.com/gl-ot/light-mq/core/message/index"
	"github.com/gl-ot/light-mq/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	topic   = "my-topic"
	message = "sup bud?"
)

func TestGetAllFrom(t *testing.T) {
	err := testutil.LogSetup("msgservice")
	if err != nil {
		t.Fatal(err)
	}
	index.InitIndex()

	firstMessage := []byte(message + "_1")
	offset, err := Store(topic, firstMessage)
	assert.Nil(t, err, "Update failed")
	assert.EqualValues(t, 0, offset, "First offset should be 0")

	secondMessage := []byte(message + "_2")
	offset, err = Store(topic, secondMessage)
	assert.Nil(t, err, "Update failed")
	assert.EqualValues(t, 1, offset, "Second offset should be 1")

	messages, err := GetAllFrom(topic, 0)
	assert.Nil(t, err)
	assert.Len(t, messages, 2)

	assert.Equal(t, Message{Offset: 0, Body: firstMessage}, *messages[0])
	assert.Equal(t, Message{Offset: 1, Body: secondMessage}, *messages[1])
}
