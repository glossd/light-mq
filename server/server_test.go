package main

import (
	"context"
	"github.com/gl-ot/light-mq/core/gate"
	"github.com/gl-ot/light-mq/core/recordlb"
	"github.com/gl-ot/light-mq/core/offset/offsetrepo"
	"github.com/gl-ot/light-mq/core/record/index"
	"github.com/gl-ot/light-mq/core/record/lmqlog"
	"github.com/gl-ot/light-mq/core/recordlb/subchan"
	"github.com/gl-ot/light-mq/proto"
	"github.com/gl-ot/light-mq/server/grpcservice"
	"github.com/gl-ot/light-mq/testutil"
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
	proto.RegisterPublisherServer(s, &grpcservice.PublishServer{})
	proto.RegisterSubscriberServer(s, &grpcservice.SubscriberServer{})
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()
}

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

func TestPublishSubscribe(t *testing.T) {
	setup(t)
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	pc := proto.NewPublisherClient(conn)
	sc := proto.NewSubscriberClient(conn)

	send(t, ctx, pc)

	subscriber := subscribe(t, ctx, sc)
	recv := recv(t, subscriber)
	gotMessage := string(recv.GetMessage())
	if gotMessage != message {
		t.Errorf("Wrong message: expecting %s, got %s", message, gotMessage)
	}
}

func TestSubscribePublish(t *testing.T) {
	setup(t)
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	pc := proto.NewPublisherClient(conn)
	sc := proto.NewSubscriberClient(conn)

	subscriber := subscribe(t, ctx, sc)
	go send(t, ctx, pc)
	recv := recv(t, subscriber)
	gotMessage := string(recv.GetMessage())
	if gotMessage != message {
		t.Errorf("Wrong message: expecting %s, got %s", message, gotMessage)
	}
}

func setup(t *testing.T) {
	err := testutil.LogSetup("server")
	if err != nil {
		t.Fatal(err)
	}
	offsetrepo.InitStorage()
	index.InitIndex()
	lmqlog.InitLogStorage()
	recordlb.Init()
	subchan.Init()
	gate.Init()
}

func send(t *testing.T, ctx context.Context, pc proto.PublisherClient) {
	_, err := pc.Send(ctx, &proto.SendRequest{Topic: topic, Message: []byte(message)})
	if err != nil {
		t.Fatalf("Sending message failed: %v", err)
	}
}

func subscribe(t *testing.T, ctx context.Context, sc proto.SubscriberClient) proto.Subscriber_SubscribeClient {
	subscribe, err := sc.Subscribe(ctx, &proto.SubscribeRequest{
		Topic: topic, Group: group,
	})
	if err != nil {
		t.Errorf("Subscription failed: %s", err)
	}

	return subscribe
}

func recv(t *testing.T, subscriber proto.Subscriber_SubscribeClient) *proto.SubscribeResponse {
	recv, err := subscriber.Recv()
	if err != nil {
		t.Errorf("Receiving failed: %s", err)
	}
	return recv
}
