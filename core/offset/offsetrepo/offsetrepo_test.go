package offsetrepo

import (
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/core/domain"
	"github.com/gl-ot/light-mq/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGet(t *testing.T) {
	err := testutil.LogSetup("offsetrepo")
	if err != nil {
		t.Fatal(err)
	}
	sg := getSG()
	config.MkDirGroup(sg.Topic, sg.Group)

	var repo = SOffsetRepo{}

	offset := repo.Get(sg)

	err = repo.Update(sg, 0)
	assert.Nil(t, err, "Update failed: ", err)

	offset = repo.Get(sg)

	assert.Equal(t, *offset, uint64(0), "Get offset should be 0")
}

func TestFillOnStartUp(t *testing.T) {
	err := testutil.LogSetup("offsetrepo")
	if err != nil {
		t.Fatal(err)
	}
	sg := getSG()
	config.MkDirGroup(sg.Topic, sg.Group)

	var repo = SOffsetRepo{}

	err = repo.Update(sg, 0)
	assert.Nil(t, err, "Update failed: ", err)
	err = repo.Update(sg, 1)
	assert.Nil(t, err, "Update failed: ", err)

	var repo2 = SOffsetRepo{}
	err = repo2.fillOffsetsOnStartUp()
	assert.Nil(t, err, "fillOffsetsOnStartUp failed")

	restoredOffset := repo2.Get(sg)
	assert.Nil(t, err, "Get failed")
	assert.Equal(t, uint64(1), *restoredOffset)
}

func getSG() domain.SGroupPartition {
	g := domain.SGroup{
		Group: "skjdffen-dsfanena",
		Topic: "dkjfalskjf-dkfjsdk-qwer",
	}
	return domain.SGroupPartition{
		SGroup: g,
		PartitionID: 0,
	}
}
