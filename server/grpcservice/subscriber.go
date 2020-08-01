package grpcservice

import (
	"github.com/gl-ot/light-mq/core"
	"github.com/gl-ot/light-mq/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SubscriberServer struct {
	proto.UnimplementedSubscriberServer
}

func (*SubscriberServer) Subscribe(in *proto.SubscribeRequest, stream proto.Subscriber_SubscribeServer) error {
	sub, err := core.NewSub(in.GetTopic(), in.GetGroup())
	if err, ok := err.(core.InputError); ok {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	err = sub.Subscribe(stream.Context(), func(msg []byte) error {
		err := stream.Send(&proto.SubscribeResponse{
			Message: msg,
		})
		if err != nil {
			log.Errorf("Couldn't send message to subscriber stream: %s", err)
		}
		return err
	})
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	return nil
}
