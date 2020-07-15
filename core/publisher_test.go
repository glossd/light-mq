package core

import (
	"github.com/gl-ot/light-mq/testutil"
	"strconv"
	"testing"
)

const (
	topic   = "my-topic"
	message = "my-message"
)

func BenchmarkPublish(b *testing.B) {
	err := testutil.LogSetup("pubsub_publisher")
	if err != nil {
		b.Fatal(err)
	}
	for n := 0; n < b.N; n++ {
		err := Publish(topic, []byte(message+"_"+strconv.Itoa(n)))
		if err != nil {
			b.Fatal("Publish failed", err)
		}
	}
}
