package main

import (
	"fmt"
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/proto"
	"github.com/gl-ot/light-mq/server/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
)

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Props.Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	reflection.Register(s)

	proto.RegisterPublisherServer(s, &service.PublishServer{})
	proto.RegisterSubscriberServer(s, &service.SubscriberServer{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
