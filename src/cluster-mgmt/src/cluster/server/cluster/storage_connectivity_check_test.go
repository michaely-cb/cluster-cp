//go:build !e2e

package cluster

import (
	pb "cerebras/pb/workflow/appliance/cluster_mgmt"
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockHTTPError implements an error that wraps an HTTP response
type mockHTTPError struct {
	resp *http.Response
}

func (e *mockHTTPError) Error() string {
	return fmt.Sprintf("HTTP error: %d", e.resp.StatusCode)
}

func (e *mockHTTPError) Response() *http.Response {
	return e.resp
}

type mockS3Client struct {
	listBucketsError error
}

func (m *mockS3Client) ListBuckets(ctx context.Context, input *s3.ListBucketsInput) (*s3.ListBucketsOutput, error) {
	if m.listBucketsError != nil {
		return nil, m.listBucketsError
	}
	return &s3.ListBucketsOutput{}, nil
}

type mockS3ClientFactory struct {
	mockClient *mockS3Client
}

func (m *mockS3ClientFactory) NewS3Client(endpoint string, regionName string) (s3Client, error) {
	return m.mockClient, nil
}

func TestCheckS3Connectivity(t *testing.T) {
	ctx, cleanup, clusterServer, client := setup(false, fakeKubeClient, mockedWsJobClient, nil, false)
	defer cleanup()

	// Note: connectivity failures are tested with a "real" client in TestCheckS3NetworkErrors

	tests := []struct {
		name           string
		request        *pb.CheckStorageConnectivityRequest
		s3Error        error
		expectedResult *pb.CheckStorageConnectivityResponse
	}{
		{
			name: "success - list buckets returns 200",
			request: &pb.CheckStorageConnectivityRequest{
				EndpointType: &pb.CheckStorageConnectivityRequest_S3Endpoint{
					S3Endpoint: &pb.S3Endpoint{
						Url: "https://192.168.1.1:9000",
					},
				},
			},
			s3Error: nil,
			expectedResult: &pb.CheckStorageConnectivityResponse{
				IsReachable: true,
				Message:     "S3 endpoint is reachable [successfully listed buckets]",
			},
		},
		{
			name: "success - list buckets returns 403",
			request: &pb.CheckStorageConnectivityRequest{
				EndpointType: &pb.CheckStorageConnectivityRequest_S3Endpoint{
					S3Endpoint: &pb.S3Endpoint{
						Url: "https://192.168.1.1:9000",
					},
				},
			},
			s3Error: &smithy.GenericAPIError{
				Code:    "AccessDenied",
				Message: "Access Denied",
				Fault:   smithy.FaultUnknown,
			},
			expectedResult: &pb.CheckStorageConnectivityResponse{
				IsReachable: true,
				Message:     "S3 endpoint is reachable [403 response]",
			},
		},
		{
			name: "success - list buckets returns InvalidAccessKeyId",
			request: &pb.CheckStorageConnectivityRequest{
				EndpointType: &pb.CheckStorageConnectivityRequest_S3Endpoint{
					S3Endpoint: &pb.S3Endpoint{
						Url: "https://192.168.1.1:9000",
					},
				},
			},
			s3Error: &smithy.GenericAPIError{
				Code:    "InvalidAccessKeyId",
				Message: "Invalid Access Key Id",
				Fault:   smithy.FaultUnknown,
			},
			expectedResult: &pb.CheckStorageConnectivityResponse{
				IsReachable: true,
				Message:     "S3 endpoint is reachable [403 response]",
			},
		},
		{
			name: "success - list buckets returns SignatureDoesNotMatch",
			request: &pb.CheckStorageConnectivityRequest{
				EndpointType: &pb.CheckStorageConnectivityRequest_S3Endpoint{
					S3Endpoint: &pb.S3Endpoint{
						Url: "https://192.168.1.1:9000",
					},
				},
			},
			s3Error: &smithy.GenericAPIError{
				Code:    "SignatureDoesNotMatch",
				Message: "Signature does not match",
				Fault:   smithy.FaultUnknown,
			},
			expectedResult: &pb.CheckStorageConnectivityResponse{
				IsReachable: true,
				Message:     "S3 endpoint is reachable [403 response]",
			},
		},
		{
			name: "success - list buckets returns InvalidKeyCredentials",
			request: &pb.CheckStorageConnectivityRequest{
				EndpointType: &pb.CheckStorageConnectivityRequest_S3Endpoint{
					S3Endpoint: &pb.S3Endpoint{
						Url: "https://192.168.1.1:9000",
					},
				},
			},
			s3Error: &smithy.GenericAPIError{
				Code:    "InvalidKeyCredentials",
				Message: "Invalid key credentials",
				Fault:   smithy.FaultUnknown,
			},
			expectedResult: &pb.CheckStorageConnectivityResponse{
				IsReachable: true,
				Message:     "S3 endpoint is reachable [403 response]",
			},
		},
		{
			name: "success with warning - list buckets returns different status code",
			request: &pb.CheckStorageConnectivityRequest{
				EndpointType: &pb.CheckStorageConnectivityRequest_S3Endpoint{
					S3Endpoint: &pb.S3Endpoint{
						Url: "https://192.168.1.1:9000",
					},
				},
			},
			s3Error: &smithy.GenericAPIError{
				Code:    "InternalServerError",
				Message: "We encountered an internal error, please try again",
			},
			expectedResult: &pb.CheckStorageConnectivityResponse{
				IsReachable: true,
				Message:     "S3 reachable, with unexpected API error [code InternalServerError]: api error InternalServerError: We encountered an internal error, please try again",
			},
		},
		{
			name: "failure - list buckets returns non-minio error",
			request: &pb.CheckStorageConnectivityRequest{
				EndpointType: &pb.CheckStorageConnectivityRequest_S3Endpoint{
					S3Endpoint: &pb.S3Endpoint{
						Url: "https://192.168.1.1:9000",
					},
				},
			},
			s3Error: fmt.Errorf("unknown error occurred"),
			expectedResult: &pb.CheckStorageConnectivityResponse{
				IsReachable: false,
				Message:     "S3 endpoint error: unknown error occurred",
			},
		},
		{
			name: "success - list buckets returns raw HTTP 403",
			request: &pb.CheckStorageConnectivityRequest{
				EndpointType: &pb.CheckStorageConnectivityRequest_S3Endpoint{
					S3Endpoint: &pb.S3Endpoint{
						Url: "https://192.168.1.1:9000",
					},
				},
			},
			s3Error: &mockHTTPError{
				resp: &http.Response{
					StatusCode: 403,
				},
			},
			expectedResult: &pb.CheckStorageConnectivityResponse{
				IsReachable: true,
				Message:     "S3 endpoint is reachable [403 response]",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Pass in a mock s3 client maker
			clusterServer.s3ClientFactory = &mockS3ClientFactory{
				mockClient: &mockS3Client{
					listBucketsError: test.s3Error,
				},
			}
			result, err := client.CheckStorageConnectivity(ctx, test.request)
			require.NoError(t, err)
			assert.Equal(t, test.expectedResult.IsReachable, result.IsReachable)
			assert.Equal(t, test.expectedResult.Message, result.Message)
		})
	}
}

func TestCheckS3NetworkErrors(t *testing.T) {
	// A set of tests with a real client against bad hosts
	ctx, cleanup, clusterServer, client := setup(false, fakeKubeClient, mockedWsJobClient, nil, false)
	clusterServer.s3ClientFactory = &prodS3ClientFactory{}

	defer cleanup()

	tests := []struct {
		name                   string
		endpoint               *pb.S3Endpoint
		expectedErrorFragments []string
	}{
		{
			name: "non-existent host",
			endpoint: &pb.S3Endpoint{
				Url: "http://garbled-nonsense-ASDF$#@#$.com:9000",
			},
			expectedErrorFragments: []string{
				"context deadline exceeded",
				"no such host",
			},
		},
		{
			name: "badly formed name",
			endpoint: &pb.S3Endpoint{
				Url: "http://invalid:host:456",
			},
			expectedErrorFragments: []string{
				"no such host",
			},
		},
		{
			name: "invalid IP",
			endpoint: &pb.S3Endpoint{
				Url: "https://4.3.2.1:9000",
			},
			expectedErrorFragments: []string{
				"S3 endpoint is not reachable",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := client.CheckStorageConnectivity(ctx, &pb.CheckStorageConnectivityRequest{
				EndpointType: &pb.CheckStorageConnectivityRequest_S3Endpoint{
					S3Endpoint: test.endpoint,
				},
			})
			require.NoError(t, err)
			assert.False(t, result.IsReachable)
			// Verify that the error message contains at least one of the expected fragments
			// (multiple errors are possible, so we check for any of them)
			foundError := false
			for _, fragment := range test.expectedErrorFragments {
				if strings.Contains(result.Message, fragment) {
					foundError = true
					break
				}
			}
			assert.True(t, foundError, "expected error message to contain at least one of %v, but got %s", test.expectedErrorFragments, result.Message)
		})
	}
}
