package service

import (
	"context"
	mqlog "github.com/gl-ot/light-mq/log"
	"github.com/gl-ot/light-mq/proto"
	log "github.com/sirupsen/logrus"
)

type PublishServer struct {
	proto.UnimplementedPublisherServer
}

func (s *PublishServer) Send(ctx context.Context, in *proto.SendRequest) (*proto.SendResponse, error) {
	log.Debugf("Received: %s", in.GetMessage())

	if err := mqlog.Publish(in.GetTopic(), in.GetMessage()); err != nil {
		return &proto.SendResponse{Status: proto.SendResponse_FAILED}, nil
	}
	return &proto.SendResponse{Status: proto.SendResponse_OK}, nil
}
