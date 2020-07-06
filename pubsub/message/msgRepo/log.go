package msgRepo

import (
	"fmt"
	"github.com/gl-ot/light-mq/config"
	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
)

func storeMessageInLog(topic string, message []byte) error {
	topicDir := config.TopicDir(topic)
	if err := os.MkdirAll(topicDir, os.ModePerm); err != nil {
		log.Error("couldn't make topic directory ", err)
		return err
	}

	logPath := filepath.Join(topicDir, "0.log")
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer f.Close()
	if err != nil {
		log.Errorf("Couldn't open file %s: %s", logPath, err)
		return fmt.Errorf("couldn't open file %s: %s", logPath, err)
	}

	_, err = f.Write(message)
	if err != nil {
		log.Errorf("Couldn't write message %s: %s", logPath, err)
		return fmt.Errorf("couldn't write message: %s", err)
	}

	return nil
}
