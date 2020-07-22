package msgservice

import (
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/core/message/index"
	"github.com/gl-ot/light-mq/core/message/msgrepo"
)

// Saves message's position to index and the message to log.
// Then sends message to all subscribers.
// Returns offset of a saved message.
func Store(topic string, message []byte) (uint64, error) {
	if err := config.MkDirTopic(topic); err != nil {
		return 0, err
	}

	r, err := msgrepo.LogStorage.Store(topic, message)
	if err != nil {
		return 0, err
	}

	index.Index.SaveIntoMemory(topic, &index.Position{Start: r.Position, Size: r.Size})

	return r.Offset, nil
}

// Offset inclusive.
func GetAllFrom(topic string, offset uint64) ([]*Message, error) {
	position := index.Index.Get(topic, offset)
	records, err := msgrepo.GetAllFrom(topic, position.Start)
	if err != nil {
		return nil, err
	}
	return mapToMessages(records), nil
}

func mapToMessages(records []*msgrepo.Record) []*Message {
	var messages []*Message
	for _, r := range records {
		messages = append(messages, &Message{Offset: r.Offset, Body: r.Body})
	}
	return messages
}
