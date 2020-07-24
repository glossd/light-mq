package core

import (
	"context"
	"fmt"
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/core/record/index"
	"github.com/gl-ot/light-mq/core/record/lmqlog"
	"github.com/gl-ot/light-mq/core/offset/offsetrepo"
	"github.com/gl-ot/light-mq/testutil"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

const (
	defaultGroup        = "my-group"
	defaultPublishCount = 100
	topic               = "my-topic"
	message             = "my-message"
)

var publishCount int

func init() {
	log.SetFlags(0)
	log.SetOutput(testutil.StdWriter{})

	pcStr := os.Getenv("LMQ_TEST_PUBLISH_COUNT")
	pc, err := strconv.Atoi(pcStr)
	if err != nil {
		publishCount = defaultPublishCount
	} else {
		publishCount = pc
	}
}

func setup(t *testing.T, testName string) {
	err := testutil.LogSetup("pubsub_" + testName)
	if err != nil {
		t.Fatal(err)
	}
	offsetrepo.InitStorage()
	index.InitIndex()
	lmqlog.InitLogStorage()
}

func publish(t *testing.T) {
	publishWithId(t, 0)
}

func publishWithId(t *testing.T, pubId int) {
	log.Printf("Pub_%d starting publishing %d messages\n", pubId, publishCount)
	for n := 0; n < publishCount; n++ {
		err := Publish(topic, []byte(buildMsg(pubId, n)))
		if err != nil {
			t.Fatal("Publish failed", err)
		}
	}
	log.Printf("Pub_%d finished publishing %d messages\n", pubId, publishCount)
}

func subscribe(t *testing.T) {
	subscribeGroupManyPubs(t, defaultGroup, 1)
}

func subscribeGroup(t *testing.T, group string) {
	subscribeGroupManyPubs(t, group, 1)
}

func subscribeManyPubs(t *testing.T, numberOfPubs int) {
	subscribeGroupManyPubs(t, defaultGroup, numberOfPubs)
}

func subscribeGroupManyPubs(t *testing.T, group string, numberOfPubs int) {
	s := newTestSubscriber(t, group)
	defer s.Close()

	startReceivingManyPubs(t, s, numberOfPubs)
}

func newTestSubscriber(t *testing.T, group string) *Subscriber {
	s, err := NewSub(topic, group)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func startReceivingManyPubs(t *testing.T, s *Subscriber, numberOfPubs int) {
	log.Printf("%v starting subscribing %d messages\n", s, publishCount)

	// index represents pubId, and value is count of received messages
	msgCounts := make([]int, numberOfPubs)

	ctx, cancel := getContext()

	err := s.Subscribe(ctx, func(message []byte) error {
		m := string(message)
		for pubId, msgNumber := range msgCounts {
			if m == buildMsg(pubId, msgNumber) {
				msgCounts[pubId]++
				if areAllMessagesSent(msgCounts) {
					cancel()
				}
				return nil
			}
		}
		t.Fatalf("%s: message out of order: msgCounts=%v, message=%s", s, msgCounts, message)
		return nil
	})
	if err != nil {
		t.Fatalf("%s subscribe failed: %s", s, err)
	}
	log.Printf("%s finished subscribing %d messages\n", s, publishCount)

	for _, v := range msgCounts {
		assert.Equal(t, v, publishCount, s.String()+" message count wrong! (Probably deadline limit)")
	}
}

func getContext() (context.Context, context.CancelFunc) {
	var ctx context.Context
	var cancel context.CancelFunc
	if config.Props.Stdout.Level == "debug" {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
	}
	return ctx, cancel
}

func areAllMessagesSent(msgCounts []int) bool {
	all := true
	for _, v := range msgCounts {
		if v != publishCount {
			all = false
		}
	}
	return all
}

func buildMsg(pubId, msgCount int) string {
	return fmt.Sprintf("pub_%d_%s_%d", pubId, message, msgCount)
}
