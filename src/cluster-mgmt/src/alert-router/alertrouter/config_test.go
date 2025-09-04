package alertrouter

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestAlertRuleMappingsConfig(t *testing.T) {
	// Create temp dir for test files
	tmpDir, err := os.MkdirTemp("", "rule-config-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create initial config file
	configPath := filepath.Join(tmpDir, "rules.yaml")
	initialConfig := `
Alert1:
  - job
  - 2
Alert2:
  - cluster
  - 1
`
	err = os.WriteFile(configPath, []byte(initialConfig), 0644)
	if err != nil {
		t.Fatalf("Failed to write initial config: %v", err)
	}

	// Create mappings and load initial config
	mappings := NewAlertRuleMappings()
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("Failed to read config: %v", err)
	}

	err = mappings.SetMappingsFromConfig(logrus.New(), data)
	if err != nil {
		t.Fatalf("Failed to load initial config: %v", err)
	}

	// Verify initial config was read
	mapping1, ok := mappings.GetMapping("Alert1")
	assert.True(t, ok, "Should have mapping for Alert1")
	assert.Equal(t, AlertTypeJob, mapping1.Category)
	assert.Equal(t, 2, mapping1.Severity)

	mapping2, ok := mappings.GetMapping("Alert2")
	assert.True(t, ok, "Should have mapping for Alert2")
	assert.Equal(t, AlertTypeCluster, mapping2.Category)
	assert.Equal(t, 1, mapping2.Severity)

	// Update config file
	updatedConfig := `
Alert1:
  - job
  - 3
Alert3:
  - cluster
  - 2
`
	err = os.WriteFile(configPath, []byte(updatedConfig), 0644)
	if err != nil {
		t.Fatalf("Failed to write updated config: %v", err)
	}

	// Load updated config
	data, err = os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("Failed to read updated config: %v", err)
	}

	err = mappings.SetMappingsFromConfig(logrus.New(), data)
	if err != nil {
		t.Fatalf("Failed to load updated config: %v", err)
	}

	// Verify updated config was read
	mapping1, ok = mappings.GetMapping("Alert1")
	assert.True(t, ok, "Should still have mapping for Alert1")
	assert.Equal(t, AlertTypeJob, mapping1.Category, "Alert1 should be updated to job category")
	assert.Equal(t, 3, mapping1.Severity, "Alert1 should be updated to severity 3")

	mapping3, ok := mappings.GetMapping("Alert3")
	assert.True(t, ok, "Should have new mapping for Alert3")
	assert.Equal(t, AlertTypeCluster, mapping3.Category)
	assert.Equal(t, 2, mapping3.Severity)

	_, ok = mappings.GetMapping("Alert2")
	assert.False(t, ok, "Alert2 should be removed")
}

func TestClusterOperatorContactsConfig(t *testing.T) {
	// Create temp dir for test files
	tmpDir, err := os.MkdirTemp("", "contacts-config-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create initial config file
	configPath := filepath.Join(tmpDir, "contacts.yaml")
	initialConfig := `
contacts:
  email:
    - id: "ops@example.com"
      severity_threshold: 1
      alert_type: "cluster"
    - id: "dev@example.com"
      severity_threshold: 2
      alert_type: "job"
`
	err = os.WriteFile(configPath, []byte(initialConfig), 0644)
	if err != nil {
		t.Fatalf("Failed to write initial config: %v", err)
	}

	// Create contacts and load initial config
	contacts := NewClusterOperatorContacts()
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("Failed to read config: %v", err)
	}

	err = contacts.SetContactsFromConfig(logrus.New(), data)
	if err != nil {
		t.Fatalf("Failed to load initial config: %v", err)
	}

	// Verify initial config was read
	contactList := contacts.GetContacts()
	assert.Len(t, contactList, 2, "Should have 2 contacts")

	assert.Equal(t, "ops@example.com", contactList[0].Address)
	assert.Equal(t, IntPtr(1), contactList[0].SeverityThreshold)
	assert.Equal(t, AlertTypeCluster, contactList[0].AlertType)

	assert.Equal(t, "dev@example.com", contactList[1].Address)
	assert.Equal(t, IntPtr(2), contactList[1].SeverityThreshold)
	assert.Equal(t, AlertTypeJob, contactList[1].AlertType)

	// Update config file
	updatedConfig := `
contacts:
  email:
    - id: "ops@example.com"
      severity_threshold: 3
      alert_type: "job"
    - id: "sre@example.com"
      severity_threshold: 1
      alert_type: "cluster"
`
	err = os.WriteFile(configPath, []byte(updatedConfig), 0644)
	if err != nil {
		t.Fatalf("Failed to write updated config: %v", err)
	}

	// Load updated config
	data, err = os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("Failed to read updated config: %v", err)
	}

	err = contacts.SetContactsFromConfig(logrus.New(), data)
	if err != nil {
		t.Fatalf("Failed to load updated config: %v", err)
	}

	// Verify updated config was read
	contactList = contacts.GetContacts()
	assert.Len(t, contactList, 2, "Should still have 2 contacts")

	assert.Equal(t, "ops@example.com", contactList[0].Address)
	assert.Equal(t, IntPtr(3), contactList[0].SeverityThreshold, "ops contact should have updated severity")
	assert.Equal(t, AlertTypeJob, contactList[0].AlertType, "ops contact should have updated category")

	assert.Equal(t, "sre@example.com", contactList[1].Address, "should have new sre contact")
	assert.Equal(t, IntPtr(1), contactList[1].SeverityThreshold)
	assert.Equal(t, AlertTypeCluster, contactList[1].AlertType)
}
