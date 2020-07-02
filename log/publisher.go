package log

import (
	"fmt"
	"github.com/gl-ot/light-mq/config"
	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
)

func Publish(topic string, body []byte) error {
	subs, ok := topicSubs.Load(topic)
	if ok {
		for s := range subs.(map[*Subscriber]bool) {
			s.channel <- body
		}
	}
	return nil
}

// todo add persistence
func saveMessage(topic string, body []byte) error {
	topicDir := filepath.Join(config.Props.Log.Dir, topic)
	if err := os.MkdirAll(topicDir, 0644); err != nil {
		log.Error("couldn't make topic directory ", err)
		return err
	}

	logPath := filepath.Join(topicDir, fmt.Sprintf("%s.log", topic))
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer f.Close()
	if err != nil {
		log.Error("couldn't open file ", err)
		return err
	}

	_, err = f.Write(body)
	_, err = f.Write([]byte("\n"))
	if err != nil {
		log.Error("couldn't write message: ", err)
		return err
	}

	return nil
}
