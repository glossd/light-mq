package config

import (
	"os"
	"path/filepath"
)

func TopicDir(topic string) string {
	return filepath.Join(TopicsDir(), topic)
}

func MkDirTopic(topic string) error {
	err := os.MkdirAll(TopicDir(topic), 0700)
	if err != nil {
		return err
	}
	return nil
}

func TopicsDir() string {
	return filepath.Join(Props.Log.Dir, "topics")
}

func SubsDir() string {
	return filepath.Join(Props.Log.Dir, "subscribers")
}
