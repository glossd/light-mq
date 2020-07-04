package topicStorage

import (
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/pubsub/topicStorage/indexStorage"
	"os"
	"path/filepath"
)

/*
	1) saves message's position to index and the message to log
	2) sends message to all subscribers
	todo make this operation atomic
*/
func StoreMessage(topic string, message []byte) error {
	if err := indexStorage.TopicMessageIndex.SaveMessage(topic, message); err != nil {
		return err
	}
	return storeMessageInLog(topic, message)
}

func GetMessages(topic string, offset int) ([][]byte, error) {
	topicDir := config.TopicDir(topic)
	logPath := filepath.Join(topicDir, "0.log")
	f, err := os.Open(logPath)
	if os.IsNotExist(err) {
		return [][]byte{}, nil
	} else if err != nil {
		return nil, err
	}

	var messages [][]byte

	positions := indexStorage.TopicMessageIndex.GetAllPositionsFrom(topic, offset)
	if len(positions) == 0 {
		return [][]byte{}, nil
	}
	first := positions[0]
	f.Seek(int64(first.Start), 0)
	for _, p := range positions {
		b := make([]byte, p.Size)
		f.Read(b)
		messages = append(messages, b)
	}

	return messages, nil
}
