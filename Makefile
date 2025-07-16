.PHONY: client server

client:
	go build -o bin/mtfs ./pkg/cmd/client

server:
	go build -o bin/mtfs-server ./pkg/cmd/server

gen:
	protoc -I proto --go_out=. --go-grpc_out=. --grpc-gateway_out=. --go_opt=module=velda.io/mtfs --go-grpc_opt=module=velda.io/mtfs --grpc-gateway_opt=module=velda.io/mtfs proto/*.proto

test:
	go test ./pkg/vfs ./pkg/server
	go test -c -o bin/test ./pkg/test && sudo ./bin/test