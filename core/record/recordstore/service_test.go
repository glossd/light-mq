package recordstore

import (
	"github.com/gl-ot/light-mq/core/record/index"
	"github.com/gl-ot/light-mq/core/record/lmqlog"
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
	expectedR1 := lmqlog.Record{
		RecordMetaData: &lmqlog.RecordMetaData{Offset: 0, Position: 0, Size: uint32(len(firstMessage))},
		Body: firstMessage,
		Topic: topic,
		PartitionID: 0,
	}
	r, err := Store(topic, firstMessage)
	assert.Nil(t, err, "Update failed")
	assert.EqualValues(t, 0, r.Offset, "First offset should be 0")

	secondMessage := []byte(message + "_2")
	expectedR2 := lmqlog.Record{
		RecordMetaData: &lmqlog.RecordMetaData{Offset: 1, Position: 30, Size: uint32(len(secondMessage))},
		Body: secondMessage,
		Topic: topic,
		PartitionID: 0,
	}
	r, err = Store(topic, secondMessage)
	assert.Nil(t, err, "Update failed")
	assert.EqualValues(t, 1, r.Offset, "Second offset should be 1")

	records, err := GetAllFrom(topic, 0,0)
	assert.Nil(t, err)

	r1 := <-records
	assert.Equal(t, expectedR1, *r1)
	r2 := <-records
	assert.Equal(t, expectedR2, *r2)

	_, ok := <-records
	assert.False(t, ok)
}
