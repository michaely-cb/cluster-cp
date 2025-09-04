//go:build default

package lock

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

//===testing test code===

func TestMatcherBuilder(t *testing.T) {
	l := NewLockBuilder("x").
		WithCreateTime(1000).
		WithSystemRequest(2, "name:f1,f2").
		WithCoordinatorRequest(2, 8, nil).
		WithPrimaryNodeGroup().
		WithSecondaryNodeGroup(1).
		BuildGranted().
		WithSystemsGrant("system/f1").
		WithCoordinatorGrant("node/n0").
		AppendGroupGrant("ng-primary-0", "nodegroup0").
		WithGrant("wgt", "node/n1", "node/n2", "node/n3", "node/n4").
		WithGrant("act", "node/n1").
		WithGrant("chf", "node/n5").
		WithGrant("wrk", "node/n6", "node/n7").
		Build().
		AppendGroupGrant("ng-secondary", "nodegroup1").
		WithGrant("act", "node/n8").
		WithGrant("chf", "node/n9").
		Build().
		Build()

	b, err := json.Marshal(l)
	assert.NoError(t, err)
	t.Log(string(b))
}
