package cluster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
)

const (
	s3ErrorAccessDenied          = "AccessDenied"
	s3ErrorInvalidAccessKeyId    = "InvalidAccessKeyId"
	s3ErrorSignatureDoesNotMatch = "SignatureDoesNotMatch"
	s3ErrorInvalidKeyCredentials = "InvalidKeyCredentials"
)

// s3Client and s3ClientFactory allow us to use the "real" AWS S3 client when
// deployed, while using a mock client for testing
type s3Client interface {
	ListBuckets(ctx context.Context, params *s3.ListBucketsInput) (*s3.ListBucketsOutput, error)
}

type s3ClientFactory interface {
	NewS3Client(endpoint string, regionName string) (s3Client, error)
}

type prodS3ClientFactory struct{}

func (m *prodS3ClientFactory) NewS3Client(endpoint string, regionName string) (s3Client, error) {
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL: endpoint,
		}, nil
	})
	// Set default region
	if regionName == "" {
		regionName = "us-east-1"
	}

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithRegion(regionName),
		config.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.MaxAttempts = 1
			})
		}),
		config.WithCredentialsProvider(
			// We don't have any credentials for the S3 endpoint, so we just use a fake one
			credentials.NewStaticCredentialsProvider("fakeAccessKey", "fakeSecretKey", ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config: %v", err)
	}

	// Create S3 client with path style addressing
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	return &s3ClientWrapper{client: client}, nil
}

// CheckStorageConnectivity checks if the specified endpoint is reachable and returns an accompanying diagnostic message
func (server ClusterServer) CheckStorageConnectivity(
	_ context.Context,
	request *pb.CheckStorageConnectivityRequest,
) (*pb.CheckStorageConnectivityResponse, error) {
	// Currently only S3 endpoint checking is implemented
	switch endpoint := request.EndpointType.(type) {
	case *pb.CheckStorageConnectivityRequest_S3Endpoint:
		return server.checkS3Connectivity(endpoint.S3Endpoint)
	default:
		return &pb.CheckStorageConnectivityResponse{
			IsReachable: false,
			Message:     "Unsupported endpoint type",
		}, nil
	}
}

// checkS3Connectivity handles S3-specific connectivity checking
func (server ClusterServer) checkS3Connectivity(endpoint *pb.S3Endpoint) (*pb.CheckStorageConnectivityResponse, error) {
	// Create an AWS S3 client with the provided endpoint
	s3Client, err := server.s3ClientFactory.NewS3Client(endpoint.Url, endpoint.RegionName)
	if err != nil {
		return &pb.CheckStorageConnectivityResponse{
			IsReachable: false,
			Message:     fmt.Sprintf("Failed to create S3 client: %v", err),
		}, nil
	}

	// Try to list buckets - this will fail with 403 which is expected
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err == nil {
		// If we somehow got no error (unlikely without credentials), the endpoint is still reachable
		return &pb.CheckStorageConnectivityResponse{
			IsReachable: true,
			Message:     "S3 endpoint is reachable [successfully listed buckets]",
		}, nil
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return &pb.CheckStorageConnectivityResponse{
			IsReachable: false,
			Message:     fmt.Sprintf("S3 endpoint is not reachable: %v", netErr),
		}, nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case s3ErrorAccessDenied, s3ErrorInvalidAccessKeyId, s3ErrorSignatureDoesNotMatch, s3ErrorInvalidKeyCredentials:
			return &pb.CheckStorageConnectivityResponse{
				IsReachable: true,
				Message:     "S3 endpoint is reachable [403 response]",
			}, nil
		default:
			return &pb.CheckStorageConnectivityResponse{
				IsReachable: true,
				Message:     fmt.Sprintf("S3 reachable, with unexpected API error [code %s]: %v", apiErr.ErrorCode(), err),
			}, nil
		}
	}

	// If we get a Wrapped HTTP error, try to unwrap check if it's a 403
	for e := err; e != nil; e = errors.Unwrap(e) {
		if httpErr, ok := e.(interface{ Response() *http.Response }); ok {
			if resp := httpErr.Response(); resp != nil && resp.StatusCode == 403 {
				return &pb.CheckStorageConnectivityResponse{
					IsReachable: true,
					Message:     "S3 endpoint is reachable [403 response]",
				}, nil
			}
		}
	}

	// TODO: if even this fails, could fall back to raw string matching [but bad idea: very fragile!]

	// Log the error and type if we it's malformed
	log.Printf("Unexpected S3 endpoint error: %v, type: %T", err, err)

	// Unknown error type
	return &pb.CheckStorageConnectivityResponse{
		IsReachable: false,
		Message:     fmt.Sprintf("S3 endpoint error: %v", err),
	}, nil
}
