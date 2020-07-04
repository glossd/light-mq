

# Compiles protobuf in proto directory
protoc:
	@protoc \
      --go_out=Mgrpc/service_config/service_config.proto=/internal/proto/grpc_service_config:. \
      --go-grpc_out=Mgrpc/service_config/service_config.proto=/internal/proto/grpc_service_config:. \
      --go_opt=paths=source_relative \
      --go-grpc_opt=paths=source_relative \
      proto/*.proto

run_dev:
	export LMQ_STDOUT_LEVEL=debug && go run server/server.go


#
# API use cases
#

topic ?= "my-topic"
group ?= "my-group"
message ?= "Hello World!"

publish:
	grpcurl -d "{\"topic\":\"$(topic)\", \"message\":\"$$(printf "$(message)" | base64)\"}" \
	  -plaintext localhost:8383 lmq.Publisher/Send
subscribe:
	grpcurl -d "{\"topic\":\"$(topic)\", \"group\":\"$(group)\"}" \
 	  -plaintext localhost:8383 lmq.Subscriber/Subscribe


#
# Onboarding scripts
#

install_protoc:
	if [ "$(uname)" = "Darwin" ]; then make install_protoc_mac; fi
install_protoc_mac:
	brew install protobuf

# Needs "$GOPATH/bin" to be in the PATH variable
install_protoc_gen_go:
	cd /tmp &&
	git clone -b v1.30.0 https://github.com/grpc/grpc-go &&
	cd grpc-go/cmd/protoc-gen-go-grpc/ &&
	go install .

install_grpcui:
	cd && go get github.com/fullstorydev/grpcui && \
	go install github.com/fullstorydev/grpcui/cmd/grpcui

install_grpcurl:
	cd && go get github.com/fullstorydev/grpcurl && \
	go install github.com/fullstorydev/grpcurl/cmd/grpcurl
