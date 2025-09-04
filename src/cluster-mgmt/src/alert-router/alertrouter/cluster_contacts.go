package alertrouter

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

// ClusterOperatorContacts manages the list of cluster operator contacts
type ClusterOperatorContacts struct {
	contacts []ContactInfo
}

// NewClusterOperatorContacts creates a new ClusterOperatorContacts instance
func NewClusterOperatorContacts() *ClusterOperatorContacts {
	return &ClusterOperatorContacts{
		contacts: make([]ContactInfo, 0),
	}
}

// GetContacts returns the current list of contacts
func (c *ClusterOperatorContacts) GetContacts() []ContactInfo {
	return c.contacts
}

// SetContactsFromConfig updates the contacts from raw YAML config
func (c *ClusterOperatorContacts) SetContactsFromConfig(logger *logrus.Logger, configData []byte) error {
	contacts, err := LoadClusterOperatorContacts(logger, configData)
	if err != nil {
		return fmt.Errorf("failed to parse config: %v", err)
	}
	c.contacts = contacts
	logger.WithFields(logrus.Fields{
		"event":    "update_contacts",
		"contacts": contacts,
	}).Info("Updated notification contacts")
	return nil
}
