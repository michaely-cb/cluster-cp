package pkg

import (
	"context"
	"runtime/debug"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func UnaryPanicRecoveryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (resp interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Recovered from panic in unary handler: %v", r)
				debug.PrintStack()
				err = status.Errorf(codes.Internal, "internal server error, %v", r)
			}
		}()
		return handler(ctx, req)
	}
}

func StreamPanicRecoveryInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) (err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Recovered from panic in stream handler: %v", r)
				debug.PrintStack()
				err = status.Errorf(codes.Internal, "internal server error, %v", r)
			}
		}()
		return handler(srv, ss)
	}
}
