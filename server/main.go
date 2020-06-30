package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/gl-ot/light-mq/proto/lmq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
)

type server struct {
	lmq.UnimplementedPublisherServer
}

func (s *server) Send(ctx context.Context, in *lmq.MessageRequest) (*lmq.MessageResponse, error) {
	log.Printf("Received: %s", in.Body)
	return &lmq.MessageResponse{Status: lmq.MessageResponse_OK}, nil
}

func main() {
	port := flag.Int("p", 8383, "application port")
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	lmq.RegisterPublisherServer(s, &server{})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
