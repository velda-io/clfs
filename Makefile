.PHONY: client server

client:
	go build -o bin/clfs ./pkg/cmd/client

server:
	go build -o bin/clfs-server ./pkg/cmd/server

gen:
	protoc -I proto --go_out=. --go-grpc_out=. --grpc-gateway_out=. --go_opt=module=velda.io/clfs --go-grpc_opt=module=velda.io/clfs --grpc-gateway_opt=module=velda.io/clfs proto/*.proto

test:
	go test ./pkg/vfs ./pkg/server
	go test -c -o bin/test ./pkg/test && sudo ./bin/test