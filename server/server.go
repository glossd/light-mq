package main

import (
	"fmt"
	"github.com/gl-ot/light-mq/config"
	"github.com/gl-ot/light-mq/proto"
	"github.com/gl-ot/light-mq/server/grpcservice"
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

	proto.RegisterPublisherServer(s, &grpcservice.PublishServer{})
	proto.RegisterSubscriberServer(s, &grpcservice.SubscriberServer{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
