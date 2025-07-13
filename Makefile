.PHONY: server

server:
	go build -o bin/mtfs ./pkg/cmd/

gen:
	protoc -I proto --go_out=. --go-grpc_out=. --grpc-gateway_out=. --go_opt=module=velda.io/mtfs --go-grpc_opt=module=velda.io/mtfs --grpc-gateway_opt=module=velda.io/mtfs proto/*.proto