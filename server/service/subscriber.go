package service

import (
	mqlog "github.com/gl-ot/light-mq/log"
	"github.com/gl-ot/light-mq/proto"
	log "github.com/sirupsen/logrus"
)

type SubscriberServer struct {
	proto.UnimplementedSubscriberServer
}

func (*SubscriberServer) Subscribe(in *proto.SubscribeRequest, stream proto.Subscriber_SubscribeServer) error {
	log.Debugf("New subscriber for topic %s", in.GetTopic())
	sub := mqlog.NewSub(in.GetTopic())
	defer sub.Close()

	sub.Subscribe(stream.Context(), func(msg []byte) {
		err := stream.Send(&proto.SubscribeResponse{
			Message: msg,
		})
		if err != nil {
			log.Errorf("Couldn't send message to subscriber stream: %s", err)
		}
	})
	return nil
}
