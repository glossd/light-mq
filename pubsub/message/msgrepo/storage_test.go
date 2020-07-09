package msgrepo

import (
	"github.com/gl-ot/light-mq/pubsub/message/idxrepo"
	"github.com/gl-ot/light-mq/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	topic   = "my-topic"
	message = "sup bud?"
)

func TestGetAllFrom(t *testing.T) {
	testutil.LogSetup(t, "msgrepo")
	idxrepo.InitIndex()

	offset, err := Store(topic, []byte(message+"_1"))
	assert.Nil(t, err, "Store failed")
	assert.Equal(t, 0, offset, "First offset should be 0")

	offset, err = Store(topic, []byte(message+"_2"))
	assert.Nil(t, err, "Store failed")
	assert.Equal(t, 1, offset, "Second offset should be 1")

	messages, err := GetAllFrom(topic, 0)
	assert.Nil(t, err)
	assert.Len(t, messages, 2)
	assert.Equal(t, message+"_1", string(messages[0].Body))
	assert.Equal(t, 0, messages[0].Offset)
	assert.Equal(t, message+"_2", string(messages[1].Body))
	assert.Equal(t, 1, messages[1].Offset)
}
