package apiv1

//go:generate protoc --go_out=. --go-grpc_out=. -I=../../grpcapi/treedb/v1 treedb.proto
