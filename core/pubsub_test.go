package core

import (
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

func TestTwoPubsOneSub(t *testing.T) {
	setup(t, "TestTwoPubsOneSub")
	go func() {
		publishWithId(t, 0)
	}()
	go func() {
		publishWithId(t, 1)
	}()
	subscribeManyPubs(t, 2)
}
