package itest

import (
	"context"
	"github.com/gl-ot/light-mq/proto"
	"github.com/gl-ot/light-mq/server/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"log"
	"net"
	"testing"
)

// Integration tests

const (
	bufSize = 1024 * 1024
	topic   = "my.topic"
	group   = "my-group"
	message = "sup bud?"
)

var lis *bufconn.Listener

func init() {
	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()
	proto.RegisterPublisherServer(s, &service.PublishServer{})
	proto.RegisterSubscriberServer(s, &service.SubscriberServer{})
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()
}

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

func TestPublishSubscriber(t *testing.T) {
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	pc := proto.NewPublisherClient(conn)
	sc := proto.NewSubscriberClient(conn)

	send(t, ctx, pc)

	subscribe, err := sc.Subscribe(ctx, &proto.SubscribeRequest{
		Topic: topic, Group: group,
	})
	if err != nil {
		t.Errorf("Subscription failed: %s", err)
	}
	recv, err := subscribe.Recv()
	if err != nil {
		t.Errorf("Receiving failed: %s", err)
	}
	gotMessage := string(recv.GetMessage())
	if gotMessage != message {
		t.Errorf("Wrong message: expecting %s, got %s", message, gotMessage)
	}
}

func send(t *testing.T, ctx context.Context, pc proto.PublisherClient) {
	_, err := pc.Send(ctx, &proto.SendRequest{Topic: topic, Message: []byte(message)})
	if err != nil {
		t.Fatalf("Sending message failed: %v", err)
	}
}
