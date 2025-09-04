//go:build default

package ws

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"
)

func TestSummarizeDetails(t *testing.T) {
	c := &resource.Collection{}
	c.AddGroup(resource.NewGroup("g0", groupProps(true, "a")))
	addMemx(c, "g0", 12, 128)
	c.AddGroup(resource.NewGroup("g1", groupProps(true, "a")))
	addMemx(c, "g1", 11, 512)
	c.AddGroup(resource.NewGroup("g2", groupProps(false, "a")))
	addMemx(c, "g2", 4, 128)
	c.AddGroup(resource.NewGroup("g3", groupProps(false, "b")))
	addMemx(c, "g3", 12, 128)
	c.AddGroup(resource.NewGroup("g4", groupProps(false, "c")))
	addMemx(c, "g4", 2, 128) // ns=c => depop only
	c.AddGroup(resource.NewGroup("g5", groupProps(false, "d")))
	addMemx(c, "g5", 2, 128) // ns=d => depop only
	c.AddGroup(resource.NewGroup("g6", groupProps(false, "d")))
	addMemx(c, "g6", 2, 128) // ns=d => depop only

	for _, testcase := range []struct {
		name              string
		ns                string
		expectedPrimary   string
		expectedSecondary string
	}{
		{
			name:              "namespace a - pop pop depop",
			ns:                "a",
			expectedPrimary:   "g1",
			expectedSecondary: "g2",
		},
		{
			name:              "namespace b - pop",
			ns:                "b",
			expectedPrimary:   "g3",
			expectedSecondary: "g3",
		},
		{
			name: "namespace c - depop",
			ns:   "c",
		},
		{
			name: "namespace d - depop depop",
			ns:   "d",
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			sum, ok := summarizeDetails(c, testcase.ns)
			assert.True(t, sum.ActivationNode == nil)
			if !ok {
				assert.Equal(t, "", testcase.expectedPrimary)
				assert.Equal(t, "", testcase.expectedSecondary)
				assert.True(t, sum.LargestMemoryPrimaryNodeGroup == nil)
				assert.True(t, sum.SmallestMemorySecondaryNodeGroup == nil)
			} else {
				require.Equal(t, testcase.expectedPrimary, sum.LargestMemoryPrimaryNodeGroup.Name)
				memxPrimary := c.Filter(resource.NewNodeFilter("memory", 0, 0, "group:"+testcase.expectedPrimary)).List()[0]
				assert.Equal(
					t,
					int64(memxPrimary.Subresources()[resource.MemSubresource])<<20,
					sum.LargestMemoryPrimaryNodeGroup.MemoryxEffectiveMemoryBytes,
				)
				assert.EqualValues(
					t,
					memxRegularMemBytes,
					sum.LargestMemoryPrimaryNodeGroup.MemoryxMemoryBytes,
				)
				assert.EqualValues(
					t,
					memxPrimaryCount,
					sum.LargestMemoryPrimaryNodeGroup.MemoryxCount,
				)

				require.Equal(t, testcase.expectedSecondary, sum.SmallestMemorySecondaryNodeGroup.Name)
				memxSecondary := c.Filter(resource.NewNodeFilter("memory", 0, 0, "group:"+testcase.expectedSecondary)).List()[0]
				assert.Equal(
					t,
					int64(memxSecondary.Subresources()[resource.MemSubresource])<<20,
					sum.SmallestMemorySecondaryNodeGroup.MemoryxEffectiveMemoryBytes,
				)
				assert.EqualValues(
					t,
					memxRegularMemBytes,
					sum.SmallestMemorySecondaryNodeGroup.MemoryxMemoryBytes,
				)
				assert.EqualValues(
					t,
					memxSecondaryV1Count,
					sum.SmallestMemorySecondaryNodeGroup.MemoryxCount,
				)
			}
		})
	}
}

func TestSummarizeDetailsV2(t *testing.T) {
	// Test case 1: Basic functionality with all tier types
	t.Run("Basic_functionality_with_all_tiers", func(t *testing.T) {
		c := &resource.Collection{}

		// Regular memory nodes
		c.AddGroup(resource.NewGroup("regular1", groupProps(true, "a")))
		addMemx(c, "regular1", 12, 128) // 128Gi

		// Large memory nodes
		c.AddGroup(resource.NewGroup("large1", groupProps(true, "a")))
		addMemx(c, "large1", 12, 1200) // ~1000Gi

		// Secondary memory nodes
		c.AddGroup(resource.NewGroup("secondary1", groupProps(true, "a")))
		addMemx(c, "secondary1", 4, 128) // 128Gi, but with 4 nodes

		// For V2 - XLarge memory nodes
		c.AddGroup(resource.NewGroup("xlarge1", groupProps(true, "a")))
		addMemx(c, "xlarge1", 12, 2350) // ~2.35Ti

		// Add AX nodes for V2
		addAx(c, "ax1", 4, 755, 400, "a")

		// Test V1 networking
		t.Run("V1_networking", func(t *testing.T) {
			wsapisv1.IsV2Network = false
			wsapisv1.UseAxScheduling = false

			details, ok := summarizeDetailsV2(c, "a")
			require.True(t, ok)
			require.NotNil(t, details)

			// Should have 3 tiers in V1: regular, large, and secondary
			require.Equal(t, 3, len(details.Nodegroups))

			// Verify regular tier
			regularTier := findTierByType(details.Nodegroups, NodeGroupTierRegular)
			require.NotNil(t, regularTier)
			assert.Equal(t, int64(memxRegularMemBytes), regularTier.MemoryxMemoryBytes)
			assert.Greater(t, regularTier.MemoryxEffectiveCount, 0)
			assert.Greater(t, regularTier.Count, 0)
			assert.Equal(t, int64(128<<30), regularTier.MemoryxEffectiveMemoryBytes)

			// Verify large tier
			largeTier := findTierByType(details.Nodegroups, NodeGroupTierLarge)
			require.NotNil(t, largeTier)
			assert.Equal(t, int64(memxLargeMemBytes), largeTier.MemoryxMemoryBytes)
			assert.Greater(t, largeTier.MemoryxEffectiveCount, 0)
			assert.Greater(t, largeTier.Count, 0)
			assert.Equal(t, int64(1200<<30), largeTier.MemoryxEffectiveMemoryBytes)

			// Verify secondary tier
			secondaryTier := findTierByType(details.Nodegroups, NodeGroupTierSecondary)
			require.NotNil(t, secondaryTier)
			assert.Equal(t, int64(memxRegularMemBytes), secondaryTier.MemoryxMemoryBytes)
			assert.Equal(t, memxSecondaryV1Count, secondaryTier.MemoryxCount)
			assert.Equal(t, 1, secondaryTier.Count)                 // We added 1 secondary nodegroup
			assert.Equal(t, 4, secondaryTier.MemoryxEffectiveCount) // 4 nodes in the secondary nodegroup
			assert.Equal(t, int64(128<<30), secondaryTier.MemoryxEffectiveMemoryBytes)

			// Activation nodes should be empty in V1
			assert.Empty(t, details.ActivationNodes)
		})

		// Test V2 networking
		t.Run("V2_networking", func(t *testing.T) {
			wsapisv1.IsV2Network = true
			wsapisv1.UseAxScheduling = true
			wsapisv1.AxCountPerSystem = 1
			defer func() {
				wsapisv1.IsV2Network = false
				wsapisv1.UseAxScheduling = false
				wsapisv1.AxCountPerSystem = 0
			}()

			details, ok := summarizeDetailsV2(c, "a")
			require.True(t, ok)
			require.NotNil(t, details)

			// Should have 3 tiers in V2: regular, large, and xlarge
			require.Equal(t, 3, len(details.Nodegroups))

			// Verify regular tier
			regularTier := findTierByType(details.Nodegroups, NodeGroupTierRegular)
			require.NotNil(t, regularTier)
			assert.Greater(t, regularTier.Count, 0)
			assert.Equal(t, int64(memxRegularMemBytes), regularTier.MemoryxMemoryBytes)
			assert.Greater(t, regularTier.MemoryxEffectiveCount, 0)
			assert.Equal(t, int64(128<<30), regularTier.MemoryxEffectiveMemoryBytes)

			// Verify large tier
			largeTier := findTierByType(details.Nodegroups, NodeGroupTierLarge)
			require.NotNil(t, largeTier)
			assert.Greater(t, regularTier.Count, 0)
			assert.Equal(t, int64(memxLargeMemBytes), largeTier.MemoryxMemoryBytes)
			assert.Greater(t, largeTier.MemoryxEffectiveCount, 0)
			assert.Equal(t, int64(1200<<30), largeTier.MemoryxEffectiveMemoryBytes)

			// Verify xlarge tier
			xlargeTier := findTierByType(details.Nodegroups, NodeGroupTierXLarge)
			require.NotNil(t, xlargeTier)
			assert.Greater(t, regularTier.Count, 0)
			assert.Equal(t, int64(memxXLargeMemBytes), xlargeTier.MemoryxMemoryBytes)
			assert.Greater(t, xlargeTier.MemoryxEffectiveCount, 0)
			assert.Equal(t, int64(2350<<30), xlargeTier.MemoryxEffectiveMemoryBytes)

			// Verify secondary tier does not exist in V2
			secondaryTier := findTierByType(details.Nodegroups, NodeGroupTierSecondary)
			assert.Nil(t, secondaryTier)

			// Verify activation nodes are present in V2
			require.Equal(t, 1, len(details.ActivationNodes))
			activationTier := details.ActivationNodes[0]
			assert.Equal(t, NodeGroupTierRegular, activationTier.Type)
			assert.Greater(t, activationTier.Count, 0)
			assert.Equal(t, int64(axMemoryBytes), activationTier.ActivationMemoryBytes)
			assert.Equal(t, 4, activationTier.Count) // We added 4 AX nodes
			assert.Equal(t, wsapisv1.AxCountPerSystem, activationTier.ActivationEffectiveCountPerCsx)
			assert.Equal(t, int64(755<<30), activationTier.ActivationEffectiveMemoryBytes)
			assert.Equal(t, int64(nicCountPerAx), activationTier.NicCount)
			assert.Equal(t, int64(nicCountPerAx), activationTier.NicEffectiveCount)
			assert.Equal(t, int64(wsapisv1.DefaultAxBandwidthBps), activationTier.NicBandwidthBps)
			assert.Equal(t, int64(wsapisv1.DefaultAxBandwidthBps), activationTier.NicEffectiveBandwidthBps)
		})
	})

	// Test case 2: Missing tier types
	t.Run("Missing_tier_types", func(t *testing.T) {
		c := &resource.Collection{}

		// Regular memory nodes only
		c.AddGroup(resource.NewGroup("regular1", groupProps(true, "a")))
		addMemx(c, "regular1", 12, 128) // 128Gi

		// Add AX nodes for V2
		addAx(c, "ax1", 2, 755, 400, "a")

		// Test V1 networking
		t.Run("V1_networking_with_regular_tier_only", func(t *testing.T) {
			wsapisv1.IsV2Network = false
			wsapisv1.UseAxScheduling = false

			details, ok := summarizeDetailsV2(c, "a")
			require.True(t, ok)
			require.NotNil(t, details)

			// Should have 3 tiers in V1: regular, large, and secondary, but only regular available
			require.Equal(t, 3, len(details.Nodegroups))

			// Verify regular tier is available
			regularTier := findTierByType(details.Nodegroups, NodeGroupTierRegular)
			require.NotNil(t, regularTier)
			assert.Greater(t, regularTier.Count, 0)

			// Verify large tier is defined but not available
			largeTier := findTierByType(details.Nodegroups, NodeGroupTierLarge)
			require.NotNil(t, largeTier)
			assert.Equal(t, 0, largeTier.Count)
			assert.Equal(t, 0, largeTier.MemoryxEffectiveCount)
			assert.Equal(t, int64(0), largeTier.MemoryxEffectiveMemoryBytes)

			// Verify secondary tier is defined but not available
			secondaryTier := findTierByType(details.Nodegroups, NodeGroupTierSecondary)
			require.NotNil(t, secondaryTier)
			assert.Equal(t, 0, secondaryTier.Count)
		})

		// Test V2 networking
		t.Run("V2_networking_with_regular_tier_only", func(t *testing.T) {
			wsapisv1.IsV2Network = true
			wsapisv1.UseAxScheduling = true
			wsapisv1.AxCountPerSystem = 1
			defer func() {
				wsapisv1.IsV2Network = false
				wsapisv1.UseAxScheduling = false
				wsapisv1.AxCountPerSystem = 0
			}()

			details, ok := summarizeDetailsV2(c, "a")
			require.True(t, ok)
			require.NotNil(t, details)

			// Should have 3 tiers in V2, but only regular available
			require.Equal(t, 3, len(details.Nodegroups))

			// Verify regular tier is available
			regularTier := findTierByType(details.Nodegroups, NodeGroupTierRegular)
			require.NotNil(t, regularTier)
			assert.Greater(t, regularTier.Count, 0)

			// Verify large tier is defined but not available
			largeTier := findTierByType(details.Nodegroups, NodeGroupTierLarge)
			require.NotNil(t, largeTier)
			assert.Equal(t, 0, largeTier.Count)

			// Verify xlarge tier is defined but not available
			xlargeTier := findTierByType(details.Nodegroups, NodeGroupTierXLarge)
			require.NotNil(t, xlargeTier)
			assert.Equal(t, 0, xlargeTier.Count)

			// Verify secondary tier does not exist in V2
			secondaryTier := findTierByType(details.Nodegroups, NodeGroupTierSecondary)
			assert.Nil(t, secondaryTier)
		})
	})

	// Test case 3: Multiple nodegroups within same tier
	t.Run("Multiple_nodegroups_within_same_tier", func(t *testing.T) {
		// Test for V1 networking
		t.Run("V1_networking", func(t *testing.T) {
			c := &resource.Collection{}

			// Multiple regular memory nodegroups with different memory sizes
			c.AddGroup(resource.NewGroup("regular1", groupProps(true, "a")))
			addMemx(c, "regular1", 12, 100) // 100Gi

			c.AddGroup(resource.NewGroup("regular2", groupProps(true, "a")))
			addMemx(c, "regular2", 12, 120) // 120Gi (a bit larger, but still regular)

			// Multiple large memory nodegroups
			c.AddGroup(resource.NewGroup("large1", groupProps(true, "a")))
			addMemx(c, "large1", 12, 1000) // 1000Gi

			c.AddGroup(resource.NewGroup("large2", groupProps(true, "a")))
			addMemx(c, "large2", 12, 1200) // 1200Gi (larger, but still in large tier)

			// Secondary memory nodegroup
			c.AddGroup(resource.NewGroup("secondary1", groupProps(true, "a")))
			addMemx(c, "secondary1", 4, 128) // 128Gi, but with 4 nodes

			wsapisv1.IsV2Network = false
			wsapisv1.UseAxScheduling = false

			details, ok := summarizeDetailsV2(c, "a")
			require.True(t, ok)
			require.NotNil(t, details)

			// Should have 3 tiers in V1: regular, large, and secondary
			require.Equal(t, 3, len(details.Nodegroups))

			// Check regular tier properly combines both regular nodegroups
			regularTier := findTierByType(details.Nodegroups, NodeGroupTierRegular)
			require.NotNil(t, regularTier)
			assert.Greater(t, regularTier.Count, 0)

			// Should count both regular nodegroups
			assert.Equal(t, 2, regularTier.Count)

			// Should be the memoryx effective count for the first nodegroup
			assert.Equal(t, 12, regularTier.MemoryxEffectiveCount)

			// Should take the smallest effective memory from the nodegroups (100Gi)
			assert.Equal(t, int64(100<<30), regularTier.MemoryxEffectiveMemoryBytes)

			// Check large tier properly combines both large nodegroups
			largeTier := findTierByType(details.Nodegroups, NodeGroupTierLarge)
			require.NotNil(t, largeTier)
			assert.Greater(t, largeTier.Count, 0)

			// Should count both large nodegroups
			assert.Equal(t, 2, largeTier.Count)

			// Should be the memoryx effective count for the first nodegroup
			assert.Equal(t, 12, largeTier.MemoryxEffectiveCount)

			// Should take the smallest effective memory from the nodegroups (1000Gi)
			assert.Equal(t, int64(1000<<30), largeTier.MemoryxEffectiveMemoryBytes)

			// Check secondary tier exists and has nodes
			secondaryTier := findTierByType(details.Nodegroups, NodeGroupTierSecondary)
			require.NotNil(t, secondaryTier)
			assert.Equal(t, 1, secondaryTier.Count)
			assert.Equal(t, 4, secondaryTier.MemoryxEffectiveCount)
			assert.Equal(t, int64(128<<30), secondaryTier.MemoryxEffectiveMemoryBytes)
		})

		// Test for V2 networking
		t.Run("V2_networking", func(t *testing.T) {
			c := &resource.Collection{}

			// Multiple regular memory nodegroups with different memory sizes
			c.AddGroup(resource.NewGroup("regular1", groupProps(true, "a")))
			addMemx(c, "regular1", 12, 128) // 128Gi

			c.AddGroup(resource.NewGroup("regular2", groupProps(true, "a")))
			addMemx(c, "regular2", 12, 160) // 160Gi (a bit larger, but still regular)

			// Multiple large memory nodegroups
			c.AddGroup(resource.NewGroup("large1", groupProps(true, "a")))
			addMemx(c, "large1", 12, 1300) // 1300Gi (V2 large tier)

			c.AddGroup(resource.NewGroup("large2", groupProps(true, "a")))
			addMemx(c, "large2", 12, 1500) // 1500Gi (larger, but still in large tier)

			// Multiple xlarge memory nodegroups
			c.AddGroup(resource.NewGroup("xlarge1", groupProps(true, "a")))
			addMemx(c, "xlarge1", 12, 2300) // 2300Gi (V2 xlarge tier)

			c.AddGroup(resource.NewGroup("xlarge2", groupProps(true, "a")))
			addMemx(c, "xlarge2", 12, 2500) // 2500Gi (larger, but still in xlarge tier)

			wsapisv1.IsV2Network = true
			wsapisv1.UseAxScheduling = false
			defer func() {
				wsapisv1.IsV2Network = false
			}()

			details, ok := summarizeDetailsV2(c, "a")
			require.True(t, ok)
			require.NotNil(t, details)

			// Should have 3 tiers in V2
			require.Equal(t, 3, len(details.Nodegroups))

			// Check regular tier properly combines both regular nodegroups
			regularTier := findTierByType(details.Nodegroups, NodeGroupTierRegular)
			require.NotNil(t, regularTier)
			assert.Greater(t, regularTier.Count, 0)

			// Should count both regular nodegroups
			assert.Equal(t, 2, regularTier.Count)

			// Should be the memoryx effective count for the first nodegroup
			assert.Equal(t, 12, regularTier.MemoryxEffectiveCount)

			// Should take the smallest effective memory from the nodegroups (128Gi)
			assert.Equal(t, int64(128<<30), regularTier.MemoryxEffectiveMemoryBytes)

			// Check large tier properly combines both large nodegroups
			largeTier := findTierByType(details.Nodegroups, NodeGroupTierLarge)
			require.NotNil(t, largeTier)
			assert.Greater(t, largeTier.Count, 0)

			// Should count both large nodegroups
			assert.Equal(t, 2, largeTier.Count)

			// Should be the memoryx effective count for the first nodegroup
			assert.Equal(t, 12, largeTier.MemoryxEffectiveCount)

			// Should take the smallest effective memory from the nodegroups (1300Gi)
			assert.Equal(t, int64(1300<<30), largeTier.MemoryxEffectiveMemoryBytes)

			// Check xlarge tier properly combines both xlarge nodegroups
			xlargeTier := findTierByType(details.Nodegroups, NodeGroupTierXLarge)
			require.NotNil(t, xlargeTier)
			assert.Greater(t, xlargeTier.Count, 0)

			// Should count both xlarge nodegroups
			assert.Equal(t, 2, xlargeTier.Count)

			// Should be the memoryx effective count for the first nodegroup
			assert.Equal(t, 12, xlargeTier.MemoryxEffectiveCount)

			// Should take the smallest effective memory from the nodegroups (2300Gi)
			assert.Equal(t, int64(2300<<30), xlargeTier.MemoryxEffectiveMemoryBytes)
		})
	})

	// Test case 4: Secondary tier condition with single vs multiple nodegroups
	t.Run("Secondary_tier_condition_with_nodegroup_count_V1_network", func(t *testing.T) {
		// Test with single nodegroup
		t.Run("Single_nodegroup_with_4_nodes_should_be_secondary", func(t *testing.T) {
			c := &resource.Collection{}

			// Single nodegroup with 4 nodes
			c.AddGroup(resource.NewGroup("g0", groupProps(true, "a")))
			addMemx(c, "g0", 4, 128) // 4 nodes (equal to memxSecondaryV1Count)

			wsapisv1.IsV2Network = false
			details, ok := summarizeDetailsV2(c, "a")
			require.True(t, ok)
			require.NotNil(t, details)

			// Should be in regular tier since it's the only nodegroup
			regularTier := findTierByType(details.Nodegroups, NodeGroupTierRegular)
			require.NotNil(t, regularTier)
			assert.Equal(t, 0, regularTier.Count)
			assert.Equal(t, 0, regularTier.MemoryxEffectiveCount)

			// Should be in secondary tier
			secondaryTier := findTierByType(details.Nodegroups, NodeGroupTierSecondary)
			require.NotNil(t, secondaryTier)
			assert.Equal(t, 1, secondaryTier.Count)
		})

		// Test with multiple nodegroups
		t.Run("4-node_nodegroup_should_be_secondary_with_multiple_nodegroups", func(t *testing.T) {
			c := &resource.Collection{}

			// First nodegroup with 12 nodes
			c.AddGroup(resource.NewGroup("g0", groupProps(true, "a")))
			addMemx(c, "g0", 12, 128)

			// Second nodegroup with 4 nodes
			c.AddGroup(resource.NewGroup("g1", groupProps(true, "a")))
			addMemx(c, "g1", 4, 128) // 4 nodes (equal to memxSecondaryV1Count)

			wsapisv1.IsV2Network = false
			details, ok := summarizeDetailsV2(c, "a")
			require.True(t, ok)
			require.NotNil(t, details)

			// First group should be in regular tier
			regularTier := findTierByType(details.Nodegroups, NodeGroupTierRegular)
			require.NotNil(t, regularTier)
			assert.Equal(t, 1, regularTier.Count)
			assert.Equal(t, 12, regularTier.MemoryxEffectiveCount)

			// Second group should be in secondary tier
			secondaryTier := findTierByType(details.Nodegroups, NodeGroupTierSecondary)
			require.NotNil(t, secondaryTier)
			assert.Equal(t, 1, secondaryTier.Count)
			assert.Equal(t, 4, secondaryTier.MemoryxEffectiveCount)
		})
	})

	// Test case 5: Empty memxGroups should return empty tiers
	t.Run("Empty_memxGroups_should_return_empty_tiers", func(t *testing.T) {
		c := &resource.Collection{}

		// Test V1 networking
		t.Run("V1_networking", func(t *testing.T) {
			wsapisv1.IsV2Network = false
			details, ok := summarizeDetailsV2(c, "a")
			require.True(t, ok)
			require.NotNil(t, details)

			// Should have all V1 tiers defined but empty
			require.Equal(t, 3, len(details.Nodegroups))

			regularTier := findTierByType(details.Nodegroups, NodeGroupTierRegular)
			require.NotNil(t, regularTier)
			assert.Equal(t, 0, regularTier.Count)
			assert.Equal(t, 0, regularTier.MemoryxEffectiveCount)
			assert.Equal(t, int64(0), regularTier.MemoryxEffectiveMemoryBytes)

			largeTier := findTierByType(details.Nodegroups, NodeGroupTierLarge)
			require.NotNil(t, largeTier)
			assert.Equal(t, 0, largeTier.Count)

			secondaryTier := findTierByType(details.Nodegroups, NodeGroupTierSecondary)
			require.NotNil(t, secondaryTier)
			assert.Equal(t, 0, secondaryTier.Count)
		})

		// Test V2 networking
		t.Run("V2_networking", func(t *testing.T) {
			wsapisv1.IsV2Network = true
			details, ok := summarizeDetailsV2(c, "a")
			require.True(t, ok)
			require.NotNil(t, details)

			// Should have all V2 tiers defined but empty
			require.Equal(t, 3, len(details.Nodegroups))

			regularTier := findTierByType(details.Nodegroups, NodeGroupTierRegular)
			require.NotNil(t, regularTier)
			assert.Equal(t, 0, regularTier.Count)
			assert.Equal(t, 0, regularTier.MemoryxEffectiveCount)
			assert.Equal(t, int64(0), regularTier.MemoryxEffectiveMemoryBytes)

			largeTier := findTierByType(details.Nodegroups, NodeGroupTierLarge)
			require.NotNil(t, largeTier)
			assert.Equal(t, 0, largeTier.Count)

			xlargeTier := findTierByType(details.Nodegroups, NodeGroupTierXLarge)
			require.NotNil(t, xlargeTier)
			assert.Equal(t, 0, xlargeTier.Count)

			// Secondary tier should not exist in V2
			secondaryTier := findTierByType(details.Nodegroups, NodeGroupTierSecondary)
			assert.Nil(t, secondaryTier)

			// Activation nodes array should be empty but defined
			assert.Empty(t, details.ActivationNodes)
		})
	})
}

// Helper function to find a tier by type
func findTierByType(tiers []*NodeGroupTier, tierType string) *NodeGroupTier {
	for _, tier := range tiers {
		if tier.Type == tierType {
			return tier
		}
	}
	return nil
}

func TestSummarizeDetails_EffectiveMemoryLessThanMemory(t *testing.T) {
	c := &resource.Collection{}
	c.AddGroup(resource.NewGroup("g0", groupProps(true, "a")))
	addMemx(c, "g0", 12, 128)
	c.AddGroup(resource.NewGroup("g1", groupProps(false, "a")))
	addMemx(c, "g1", 4, 100)
	c.AddGroup(resource.NewGroup("g2", groupProps(true, "b")))
	addMemx(c, "g2", 12, 100)
	c.AddGroup(resource.NewGroup("g3", groupProps(false, "b")))
	addMemx(c, "g3", 4, 128)
	c.AddGroup(resource.NewGroup("g4", groupProps(true, "c")))
	addMemx(c, "g4", 12, 188)
	addAx(c, "g03", 2, 700, 400, "c")

	{
		rdA, ok := summarizeDetails(c, "a")
		require.True(t, ok)
		err := rdA.sanityCheck()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Smallest memory nodegroup")
	}

	{
		rdB, ok := summarizeDetails(c, "b")
		require.True(t, ok)
		err := rdB.sanityCheck()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Largest memory nodegroup")
	}

	{
		wsapisv1.IsV2Network = true
		wsapisv1.UseAxScheduling = true
		defer func() {
			wsapisv1.IsV2Network = false
			wsapisv1.UseAxScheduling = false
			wsapisv1.AxCountPerSystem = 0
		}()
		rdC, ok := summarizeDetails(c, "c")
		require.True(t, ok)
		err := rdC.sanityCheck()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Activation node")
	}
}

func TestSummarizeDetails_V2Networking(t *testing.T) {
	wsapisv1.IsV2Network = true
	wsapisv1.UseAxScheduling = true
	defer func() {
		wsapisv1.IsV2Network = false
		wsapisv1.UseAxScheduling = false
		wsapisv1.AxCountPerSystem = 0
	}()

	// do not add memx nodes to depop groups for v2 networking
	c := &resource.Collection{}
	c.AddGroup(resource.NewGroup("g0", groupProps(true, "a")))
	addMemx(c, "g0", 12, 128)
	c.AddGroup(resource.NewGroup("g1", groupProps(true, "a")))
	addMemx(c, "g1", 11, 512)
	addAx(c, "g00", 4, 128, 400, "a")
	c.AddGroup(resource.NewGroup("g2", groupProps(false, "a")))
	addMemx(c, "g2", 4, 512)

	c.AddGroup(resource.NewGroup("g3", groupProps(false, "b")))
	addMemx(c, "g3", 12, 128)
	addAx(c, "g01", 3, 128, 100, "b")
	addAx(c, "g02", 1, 64, 100, "b")

	c.AddGroup(resource.NewGroup("g4", groupProps(false, "c")))
	addAx(c, "g03", 2, 64, 400, "c")
	addAx(c, "g04", 2, 128, 100, "c")

	c.AddGroup(resource.NewGroup("g5", groupProps(false, "d")))
	c.AddGroup(resource.NewGroup("g6", groupProps(false, "d")))

	for _, testcase := range []struct {
		name              string
		ns                string
		axCountPerSystem  int
		expectedPrimary   string
		expectedMemxBwBps int64
		expectedSecondary string
		expectedAx        string
		expectedAxMem     int
		expectedAxBwBps   int64
	}{
		{
			name:              "namespace a - pop pop depop ax",
			ns:                "a",
			axCountPerSystem:  1,
			expectedPrimary:   "g1",
			expectedMemxBwBps: wsapisv1.DefaultBandwidthBps,
			expectedSecondary: "g2",
			expectedAx:        "ax0-g00",
			expectedAxMem:     128,
			expectedAxBwBps:   wsapisv1.DefaultAxBandwidthBps,
		},
		{
			name:              "namespace b - pop ax",
			ns:                "b",
			axCountPerSystem:  1,
			expectedPrimary:   "g3",
			expectedMemxBwBps: wsapisv1.DefaultBandwidthBps,
			expectedSecondary: "g3",
			expectedAx:        "ax0-g01",
			expectedAxMem:     64,
			expectedAxBwBps:   wsapisv1.DefaultBandwidthBps,
		},
		{
			name:             "namespace c - depop ax",
			ns:               "c",
			axCountPerSystem: 4,
			expectedAx:       "ax0-g04",
			expectedAxMem:    64,
			expectedAxBwBps:  wsapisv1.DefaultBandwidthBps,
		},
		{
			name:             "namespace d - depop depop",
			ns:               "d",
			axCountPerSystem: 1,
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			wsapisv1.AxCountPerSystem = testcase.axCountPerSystem
			sum, ok := summarizeDetails(c, testcase.ns)
			require.Equal(t, testcase.expectedPrimary != "", ok)

			if testcase.expectedPrimary != "" {
				require.Equal(t, testcase.expectedPrimary, sum.LargestMemoryPrimaryNodeGroup.Name)
				require.Equal(t, testcase.expectedMemxBwBps, sum.LargestMemoryPrimaryNodeGroup.MemoryxBandwidthBps)
				memxPrimary := c.Filter(resource.NewNodeFilter("memory", 0, 0, "group:"+testcase.expectedPrimary)).List()[0]
				assert.Equal(
					t,
					int64(memxPrimary.Subresources()[resource.MemSubresource])<<20,
					sum.LargestMemoryPrimaryNodeGroup.MemoryxEffectiveMemoryBytes,
				)
				assert.EqualValues(
					t,
					memxRegularMemBytes,
					sum.LargestMemoryPrimaryNodeGroup.MemoryxMemoryBytes,
				)
				assert.EqualValues(
					t,
					memxPrimaryCount,
					sum.LargestMemoryPrimaryNodeGroup.MemoryxCount,
				)
			} else {
				require.True(t, sum.LargestMemoryPrimaryNodeGroup == nil)
			}

			// Setting the secondary nodegroup in V2 networking clusters only for backward compatibility
			// TODO: In rel-2.5, set SmallestMemorySecondaryNodeGroup only in V1 networking clusters
			if testcase.expectedSecondary != "" {
				require.Equal(t, testcase.expectedSecondary, sum.SmallestMemorySecondaryNodeGroup.Name)
				require.Equal(t, testcase.expectedMemxBwBps, sum.SmallestMemorySecondaryNodeGroup.MemoryxBandwidthBps)
				memxSecondary := c.Filter(resource.NewNodeFilter("memory", 0, 0, "group:"+testcase.expectedSecondary)).List()[0]
				assert.Equal(
					t,
					int64(memxSecondary.Subresources()[resource.MemSubresource])<<20,
					sum.SmallestMemorySecondaryNodeGroup.MemoryxEffectiveMemoryBytes,
				)
				assert.EqualValues(
					t,
					memxRegularMemBytes,
					sum.SmallestMemorySecondaryNodeGroup.MemoryxMemoryBytes,
				)
				assert.EqualValues(
					t,
					memxSecondaryV1Count,
					sum.SmallestMemorySecondaryNodeGroup.MemoryxCount,
				)
			} else {
				require.True(t, sum.SmallestMemorySecondaryNodeGroup == nil)
			}

			if testcase.expectedAx != "" {
				require.Equal(t, testcase.expectedAx, sum.ActivationNode.Name)
				require.Equal(t, axCountPerCsxDefault, sum.ActivationNode.ActivationCountPerCsx)
				require.Equal(t, testcase.axCountPerSystem, sum.ActivationNode.ActivationEffectiveCountPerCsx)
				require.Equal(t, int64(nicCountPerAx), sum.ActivationNode.NicCount)
				require.Equal(t, int64(nicCountPerAx), sum.ActivationNode.NicEffectiveCount)
				require.Equal(t, int64(wsapisv1.DefaultAxBandwidthBps), sum.ActivationNode.NicBandwidthBps)
				require.Equal(t, testcase.expectedAxBwBps, sum.ActivationNode.NicEffectiveBandwidthBps)
				require.Equal(t, int64(wsapisv1.DefaultAxBandwidthBps), sum.ActivationNode.ActivationBandwidthBps)
				require.Equal(t, testcase.expectedAxBwBps, sum.ActivationNode.ActivationEffectiveBandwidthBps)

				ax, ok := c.Get(fmt.Sprintf("%s/%s", resource.NodeType, testcase.expectedAx))
				assert.True(t, ok)
				assert.Equal(
					t,
					int64(ax.Subresources()[resource.MemSubresource])<<20,
					sum.ActivationNode.ActivationEffectiveMemoryBytes,
				)
				assert.EqualValues(
					t,
					axMemoryBytes,
					sum.ActivationNode.ActivationMemoryBytes,
				)
			} else {
				require.True(t, sum.ActivationNode == nil)
			}
		})
	}
}

func TestSummarizeDetailsV2_SanityChecks(t *testing.T) {
	// Test case 1: Regular tier with insufficient memory
	t.Run("Regular_tier_insufficient_memory", func(t *testing.T) {
		c := &resource.Collection{}
		c.AddGroup(resource.NewGroup("regular1", groupProps(true, "a")))
		addMemx(c, "regular1", 12, 100) // 100Gi < configured memory

		// Test V1 networking
		t.Run("V1_networking", func(t *testing.T) {
			wsapisv1.IsV2Network = false
			details, ok := summarizeDetailsV2(c, "a")
			require.True(t, ok)
			err := details.sanityCheck()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "invalid cluster resource configuration")
			assert.Contains(t, err.Error(), "regular tier nodegroup has less actual memory than the configured cluster default")
			assert.Contains(t, err.Error(), wsapisv1.ContactCerebrasSupport)
		})

		// Test V2 networking
		t.Run("V2_networking", func(t *testing.T) {
			wsapisv1.IsV2Network = true
			details, ok := summarizeDetailsV2(c, "a")
			require.True(t, ok)
			err := details.sanityCheck()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "invalid cluster resource configuration")
			assert.Contains(t, err.Error(), "regular tier nodegroup has less actual memory than the configured cluster default")
			assert.Contains(t, err.Error(), wsapisv1.ContactCerebrasSupport)
		})
	})

	// Test case 2: AX node with insufficient memory in V2
	t.Run("AX_node_insufficient_memory_in_V2", func(t *testing.T) {
		c := &resource.Collection{}
		c.AddGroup(resource.NewGroup("regular1", groupProps(true, "a")))
		addMemx(c, "regular1", 12, 200)
		addAx(c, "ax1", 4, 700, 400, "a") // 700Gi < 755Gi

		wsapisv1.IsV2Network = true
		wsapisv1.UseAxScheduling = true
		defer func() {
			wsapisv1.IsV2Network = false
			wsapisv1.UseAxScheduling = false
		}()

		details, ok := summarizeDetailsV2(c, "a")
		require.True(t, ok)
		err := details.sanityCheck()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid cluster resource configuration")
		assert.Contains(t, err.Error(), "Activation node has less actual memory than the configured cluster default")
		assert.Contains(t, err.Error(), wsapisv1.ContactCerebrasSupport)
	})

	// Test case 3: Effective memory less than configured memory
	t.Run("Effective_memory_less_than_configured", func(t *testing.T) {
		c := &resource.Collection{}
		c.AddGroup(resource.NewGroup("regular1", groupProps(true, "a")))
		// Add nodes with different memory sizes to force effective memory to be less than configured
		addMemx(c, "regular1", 12, 128)
		c.AddGroup(resource.NewGroup("regular2", groupProps(true, "a")))
		addMemx(c, "regular2", 12, 100) // This will make effective memory 100Gi < configured memory

		details, ok := summarizeDetailsV2(c, "a")
		require.True(t, ok)
		err := details.sanityCheck()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid cluster resource configuration")
		assert.Contains(t, err.Error(), "regular tier nodegroup has less actual memory than the configured cluster default")
		assert.Contains(t, err.Error(), wsapisv1.ContactCerebrasSupport)
	})
}

func addMemx(c *resource.Collection, group string, numMemx, mem int) {
	for i := 0; i < numMemx; i++ {
		c.Add(resource.NewResource(
			resource.NodeType,
			fmt.Sprintf("n%d-%s", i, group),
			map[string]string{resource.GroupPropertyKey: group, resource.RoleMemoryPropKey: ""},
			resource.NewCpuMemPodSubresources(64, mem<<10),
			nil,
		))
	}
}

func addAx(c *resource.Collection, group string, numAx, mem, bwGbps int, ns string) {
	sr := resource.NewCpuMemPodSubresources(64, mem<<10)
	sr[fmt.Sprintf("%s/nic-0", resource.NodeNicSubresource)] = bwGbps + 1
	sr[fmt.Sprintf("%s/nic-1", resource.NodeNicSubresource)] = bwGbps

	// AX does not belong to any group in production
	// "group" is only added here to help create resources with different names in tests
	for i := 0; i < numAx; i++ {
		nodeName := fmt.Sprintf("ax%d-%s", i, group)
		c.Add(resource.NewResource(
			resource.NodeType,
			nodeName,
			map[string]string{resource.RoleAxPropKey: "", common.NamespaceKey: ns},
			sr,
			nil))
	}
}

func groupProps(pop bool, ns string) map[string]string {
	return map[string]string{
		wsapisv1.MemxPopNodegroupProp: strconv.FormatBool(pop),
		common.NamespaceKey:           ns,
	}
}
