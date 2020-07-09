package offsetrepo

import (
	"github.com/gl-ot/light-mq/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBoltStorage_Store(t *testing.T) {
	testutil.LogSetup(t, "offsetrepo")
	var repo = boltStorage{db: createBoltDb()}

	sg := &SubscriberGroup{
		Group: "skjdffen-dsfanena",
		Topic: "dkjfalskjf-dkfjsdk-qwer",
	}
	offset, err := repo.GetLatest(sg)
	assert.Nil(t, offset, "GetLatest offset should be nil")

	err = repo.Store(sg, 0)
	assert.Nil(t, err, "Store failed: ", err)

	offset, err = repo.GetLatest(sg)
	assert.Nil(t, err, "Get latest failed: ", err)

	assert.Equal(t, *offset, 0, "GetLatest offset should be 0")
}
