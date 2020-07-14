package pubsub

import (
	"context"
	"github.com/gl-ot/light-mq/pubsub/message/idxrepo"
	"github.com/gl-ot/light-mq/pubsub/offset/offsetrepo"
	"github.com/gl-ot/light-mq/testutil"
	"log"
	"strconv"
	"testing"
	"time"
)

const (
	msgCount = 10000
)

func init() {
	log.SetFlags(0)
	log.SetOutput(testutil.StdWriter{})
}

func TestPubSubStreaming(t *testing.T) {
	setup(t, "streaming")
	go func() {
		publish(t)
	}()
	time.Sleep(time.Millisecond)
	subscribe(t)
}

func TestPubSubAllFromDisk(t *testing.T) {
	setup(t, "all_from_disk")
	publish(t)
	subscribe(t)
}

func TestSubBeforePub(t *testing.T) {
	setup(t, "sub_then_pub")
	go func() {
		time.Sleep(time.Second)
		publish(t)
	}()
	subscribe(t)
}

func setup(t *testing.T, testName string) {
	err := testutil.LogSetup("pubsub_" + testName)
	if err != nil {
		t.Fatal(err)
	}
	offsetrepo.InitStorage()
	idxrepo.InitIndex()
}

func publish(t *testing.T) {
	log.Printf("Starting publishing %d messages\n", msgCount)
	for n := 0; n <= msgCount; n++ {
		err := Publish(topic, []byte(message+"_"+strconv.Itoa(n)))
		if err != nil {
			t.Fatal("Publish failed", err)
		}
	}
	log.Printf("Finished publishing %d messages\n", msgCount)
}

func subscribe(t *testing.T) {
	s, err := NewSub(topic, "my-group")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	log.Printf("Starting subscribing %d messages\n", msgCount)

	i := 0
	ctx, cancel := context.WithCancel(context.Background())
	err = s.Subscribe(ctx, func(msg []byte) error {
		if string(msg) != message+"_"+strconv.Itoa(i) {
			t.Fatalf("Message out of order: i=%d, msg=%s", i, msg)
		}
		i++
		if i == msgCount {
			cancel()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %s", err)
	}
	log.Printf("Finished subscribing %d messages\n", msgCount)
}
