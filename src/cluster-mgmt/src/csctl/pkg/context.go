package pkg

import (
	"context"

	"github.com/sirupsen/logrus"
)

type CmdCtx struct {
	Context              context.Context
	Cfg                  *Config
	ClientFactory        ClientFactory
	UserIdentityProvider UserIdentityProvider
	InClusterNodeChecker InClusterNodeChecker
	Namespace            string
	DebugLevel           int
	Uid                  int64
	Gid                  int64
	Groups               []int64
	OnMgmtNode           bool
	SingleNodeCluster    bool
	Logger               *logrus.Logger
}
