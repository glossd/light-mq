package recordstore

import (
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/core/record/index"
	"github.com/gl-ot/light-mq/core/record/lmqlog"
)

// Saves the message to the log.
// Then saves in memory record's position to index
// Returns save record.
func Store(topic string, message []byte) (*lmqlog.Record, error) {
	if err := config.MkDirTopic(topic); err != nil {
		return nil, err
	}

	r, err := lmqlog.Log.Store(topic, message)
	if err != nil {
		return nil, err
	}

	index.Index.SaveIntoMemory(topic, &index.Position{Start: r.Position, Size: r.Size})

	return r, nil
}

// Offset inclusive.
func GetAllFrom(topic string, offset uint64) (<-chan *lmqlog.Record, error) {
	position := index.Index.Get(topic, offset)
	return lmqlog.Log.GetAllFrom(topic, position.Start)
}
