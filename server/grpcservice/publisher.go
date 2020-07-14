package grpcservice

import (
	"context"
	"github.com/gl-ot/light-mq/proto"
	"github.com/gl-ot/light-mq/pubsub"
	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PublishServer struct {
	proto.UnimplementedPublisherServer
}

func (s *PublishServer) Send(ctx context.Context, in *proto.SendRequest) (*empty.Empty, error) {
	log.Tracef("Received: %s", in.GetMessage())

	err := pubsub.Publish(in.GetTopic(), in.GetMessage())
	if err, ok := err.(pubsub.InputError); ok {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &empty.Empty{}, nil
}
