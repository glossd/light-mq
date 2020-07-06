package msgRepo

import (
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/pubsub/message/idxRepo"
	"os"
	"path/filepath"
)

// 1) saves message's position to index and the message to log
// 2) sends message to all subscribers
// Returns offset of a saved message
// todo make this operation atomic
func StoreMessage(topic string, message []byte) (int, error) {
	newOffset, err := idxRepo.TopicMessageIndex.SaveMessage(topic, message)
	if err != nil {
		return 0, err
	}
	err = storeMessageInLog(topic, message)
	if err != nil {
		return 0, err
	}

	return newOffset, nil
}

// Deprecated, use StreamMessagesFrom
func GetMessages(topic string, offset int) ([]*Message, error) {
	topicDir := config.TopicDir(topic)
	logPath := filepath.Join(topicDir, "0.log")
	f, err := os.Open(logPath)
	if os.IsNotExist(err) {
		return []*Message{}, nil
	} else if err != nil {
		return nil, err
	}

	positions := idxRepo.TopicMessageIndex.GetAllPositionsFrom(topic, offset)
	if len(positions) == 0 {
		return []*Message{}, nil
	}
	f.Seek(int64(positions[0].Start), 0)

	var messages []*Message
	for _, p := range positions {
		b := make([]byte, p.Size)
		f.Read(b)
		messages = append(messages, &Message{Offset: offset, Body: b})
		offset++
	}

	return messages, nil
}
