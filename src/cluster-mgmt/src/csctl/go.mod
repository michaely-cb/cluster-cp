module cerebras.com/cluster/csctl

go 1.21

toolchain go1.21.8

require (
	cerebras.com/job-operator v0.0.0
	cerebras/pb v0.0.0
	github.com/evanphx/json-patch/v5 v5.8.0
	github.com/golang/protobuf v1.5.3
	github.com/gookit/color v1.5.2
	github.com/kr/text v0.2.0
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.9.3
	github.com/spf13/cobra v1.5.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.9.0
	golang.org/x/exp v0.0.0-20231006140011-7918f672742d
	google.golang.org/grpc v1.67.3
	google.golang.org/protobuf v1.34.2
	gopkg.in/yaml.v3 v3.0.1
	k8s.io/apimachinery v0.29.2
	sigs.k8s.io/yaml v1.4.0

)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/xo/terminfo v0.0.0-20210125001918-ca9a967f8778 // indirect
	golang.org/x/net v0.28.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/text v0.17.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240814211410-ddb44dafa142 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	k8s.io/klog/v2 v2.110.1 // indirect
	k8s.io/utils v0.0.0-20230726121419-3b25d923346b // indirect
	sigs.k8s.io/controller-runtime v0.17.6 // indirect
	sigs.k8s.io/json v0.0.0-20221116044647-bc3834ca7abd // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.4.1 // indirect
)

replace cerebras/pb => ../pb

replace cerebras.com/job-operator => ../job-operator
