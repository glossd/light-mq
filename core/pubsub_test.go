package core

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestPubSubStreaming(t *testing.T) {
	setup(t, "TestPubSubStreaming")
	go func() {
		publish(t)
	}()
	time.Sleep(time.Millisecond)
	subscribe(t)
}

func TestPubSubAllFromDisk(t *testing.T) {
	setup(t, "TestPubSubAllFromDisk")
	publish(t)
	subscribe(t)
}

func TestSubBeforePub(t *testing.T) {
	setup(t, "TestSubBeforePub")
	go func() {
		time.Sleep(time.Millisecond * 10)
		publish(t)
	}()
	subscribe(t)
}

func TestOnePubManySubGroups(t *testing.T) {
	setup(t, "TestOnePubManySubGroups")
	go func() {
		publish(t)
	}()
	go func() {
		subscribeGroup(t, "my-group-1")
	}()
	subscribeGroup(t, "my-group-2")
}

func TestManyPubsOneSub(t *testing.T) {
	setup(t, "TestManyPubsOneSub")
	go func() {
		publishWithId(t, 0)
	}()
	go func() {
		publishWithId(t, 1)
	}()
	subscribeManyPubs(t, 2)
}

// todo add as test
func OnePubManySubsInGroup(t *testing.T) {
	setup(t, "TestOnePubManySubsInGroup")
	go func() {
		publish(t)
	}()
	s1 := newTestSubscriber(t, defaultGroup)
	defer s1.Close()
	s2 := newTestSubscriber(t, defaultGroup)
	defer s2.Close()

	msgCount := 0
	ctx, cancel := getContext()
	handler := func(body []byte) error {
		m := string(body)
		if m == buildMsg(0, msgCount) {
			msgCount++
			if msgCount == publishCount {
				cancel()
			}
		}
		return nil
	}
	go s1.Subscribe(ctx, handler)
	s2.Subscribe(ctx, handler)
	assert.Equal(t, publishCount, msgCount, "Message count doesn't equals publish count")
}
