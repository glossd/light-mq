package service

import (
	"context"
	"github.com/gl-ot/light-mq/proto"
	mqlog "github.com/gl-ot/light-mq/pubsub"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PublishServer struct {
	proto.UnimplementedPublisherServer
}

func (s *PublishServer) Send(ctx context.Context, in *proto.SendRequest) (*proto.SendResponse, error) {
	log.Debugf("Received: %s", in.GetMessage())

	err := mqlog.Publish(in.GetTopic(), in.GetMessage())
	if err, ok := err.(mqlog.InputError); ok {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err != nil {
		return &proto.SendResponse{Status: proto.SendResponse_FAILED}, nil
	}

	return &proto.SendResponse{Status: proto.SendResponse_OK}, nil
}
