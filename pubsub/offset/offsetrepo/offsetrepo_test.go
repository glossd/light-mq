package offsetrepo

import (
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGet(t *testing.T) {
	err := testutil.LogSetup("offsetrepo")
	if err != nil {
		t.Fatal(err)
	}
	sg := &SubscriberGroup{
		Group: "skjdffen-dsfanena",
		Topic: "dkjfalskjf-dkfjsdk-qwer",
	}
	config.MkDirGroup(sg.Topic, sg.Group)

	var repo = FileStorage{offsets: make(map[string]*uint64)}

	offset, err := repo.Get(sg)
	assert.Nil(t, offset, "Get offset should be nil")

	err = repo.Update(sg, 0)
	assert.Nil(t, err, "Update failed: ", err)

	offset, err = repo.Get(sg)
	assert.Nil(t, err, "Get offset failed: ", err)

	assert.Equal(t, *offset, uint64(0), "Get offset should be 0")
}

func TestFillOnStartUp(t *testing.T) {
	err := testutil.LogSetup("offsetrepo")
	if err != nil {
		t.Fatal(err)
	}
	sg := &SubscriberGroup{
		Group: "my-group",
		Topic: "my-topic",
	}
	config.MkDirGroup(sg.Topic, sg.Group)

	var repo = FileStorage{offsets: make(map[string]*uint64)}

	err = repo.Update(sg, 0)
	assert.Nil(t, err, "Update failed: ", err)
	err = repo.Update(sg, 1)
	assert.Nil(t, err, "Update failed: ", err)

	var repo2 = FileStorage{offsets: make(map[string]*uint64)}
	err = repo2.fillOffsetsOnStartUp()
	assert.Nil(t, err, "fillOffsetsOnStartUp failed")

	restoredOffset, err := repo2.Get(sg)
	assert.Nil(t, err, "Get failed")
	assert.Equal(t, uint64(1), *restoredOffset)
}
