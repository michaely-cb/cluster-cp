package pkg

import (
	"fmt"
	"testing"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestBuildNamespaceAwareAuthority(t *testing.T) {
	baseAuthority := "cluster-server.foxhole.cerebrassc.local"
	for _, testcase := range []struct {
		Name              string
		BaseAuthority     string
		Namespace         string
		ShouldFail        bool
		ExpectedAuthority string
	}{
		{
			Name:              "system namespace + base authority",
			BaseAuthority:     baseAuthority,
			Namespace:         "job-operator",
			ShouldFail:        false,
			ExpectedAuthority: fmt.Sprintf("job-operator.%s", baseAuthority),
		},
		{
			Name:              "user namespace + base authority",
			BaseAuthority:     baseAuthority,
			Namespace:         "user-a",
			ShouldFail:        false,
			ExpectedAuthority: fmt.Sprintf("user-a.%s", baseAuthority),
		},
		{
			Name:              "invalid base authority 1",
			BaseAuthority:     fmt.Sprintf("user-a.%s", baseAuthority),
			Namespace:         "user-a",
			ShouldFail:        true,
			ExpectedAuthority: "",
		},
		{
			Name:              "invalid base authority 2",
			BaseAuthority:     "foobar.cerebrassc.local",
			Namespace:         "user-a",
			ShouldFail:        true,
			ExpectedAuthority: "",
		},
	} {
		t.Run(testcase.Name, func(t *testing.T) {
			authority, err := buildNsAwareAuthority(testcase.BaseAuthority, testcase.Namespace)
			if testcase.ShouldFail {
				require.NotNil(t, err)
			} else {
				require.Equal(t, testcase.ExpectedAuthority, authority)
			}
		})
	}
}

func TestFormatGrpcError(t *testing.T) {
	for _, testcase := range []struct {
		Name	     		string
		ExpectedMessage 	string
		OriginalError   	error
	}{
		{
			Name: 			 "Format namespacereservation not found error",
			ExpectedMessage: "Session \"xxx\" not found",
			OriginalError: 	 grpcStatus.Errorf(codes.NotFound, "namespacereservations.jobs.cerebras.com \"xxx\" not found"),
		},
		{
			Name: 			 "Error message without internal names not formatted",
			ExpectedMessage: "XXX is not found",
			OriginalError: 	 grpcStatus.Errorf(codes.NotFound, "XXX is not found"),
		},
		{
			Name: 			 "Format without internal names but has *.jobs.cerebras.com",
			ExpectedMessage: "XXX is not found",
			OriginalError: 	 grpcStatus.Errorf(codes.NotFound, "abc.jobs.cerebras.com XXX is not found"),
		},
	} {
		t.Run(testcase.Name, func(t *testing.T) {
			err := FormatGrpcError(testcase.OriginalError)
			require.Equal(t, testcase.ExpectedMessage, err.Error())
		})
	}
}
