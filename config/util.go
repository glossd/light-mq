package config

import (
	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
)

func TopicDir(topic string) string {
	return filepath.Join(TopicsDir(), topic)
}

func MkDirTopic(topic string) error {
	err := os.MkdirAll(TopicDir(topic), 0700)
	if err != nil {
		log.Errorf("Couldn't create topic directory: %s", err)
		return err
	}
	return nil
}

func MkDirGroup(topic, group string) error {
	err := os.MkdirAll(GroupDir(topic, group), 0700)
	if err != nil {
		log.Errorf("Couldn't create group directory: %s", err)
		return err
	}
	return nil
}

func GroupDir(topic, group string) string {
	return filepath.Join(GroupsDir(topic), group)
}

func GroupsDir(topic string) string {
	return filepath.Join(TopicDir(topic), "groups")
}

func TopicsDir() string {
	return filepath.Join(Props.Log.Dir, "topics")
}
