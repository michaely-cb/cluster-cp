package alertrouter

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

// ContactType represents the type of contact information
type ContactType string

const (
	ContactTypeEmail     ContactType = "email"
	ContactTypeSlack     ContactType = "slack"
	ContactTypePagerDuty ContactType = "pagerduty"
)

// AlertType determines which group should be notified
type AlertType string

const (
	AlertTypeJob     AlertType = "job"
	AlertTypeCluster AlertType = "cluster"
	AlertTypeAll     AlertType = "all"
)

// ContactInfo represents contact information that can be extended
type ContactInfo struct {
	Type              ContactType
	Address           string
	SeverityThreshold *int      // 1-3, where 1 is most severe
	AlertType         AlertType // Optional category (job/cluster) filter
	AlertName         string    // Optional alert name override
}

// GetClusterOperatorContacts returns filtered cluster operator contacts based on category, severity and alertname,
// grouped by contact type
func GetClusterOperatorContacts(contacts []ContactInfo, alertType AlertType, severity int, alertname string) map[ContactType][]string {
	notificationTargets := make(map[ContactType][]string)

	for _, contact := range contacts {
		// Rule 1: If contact explicitly refers to any alert type, include it if it matches [otherwise, skip it]
		if contact.AlertName != "" && contact.AlertName != alertname {
			logrus.Infof("skipping contact %s because alertname does not match %s", contact.Address, alertname)
			continue
		}

		// Rule 2: If contact specifies a severity threshold, alert severity must be lower or equal
		// (lower numbers are more severe, so alert severity must be <= threshold)
		if contact.SeverityThreshold != nil && severity > *contact.SeverityThreshold {
			logrus.Infof("skipping contact %s because severity %d is greater than threshold %d", contact.Address, severity, *contact.SeverityThreshold)
			continue
		}

		// Rule 3: If contact specifies a category (job/cluster), it must match
		if contact.AlertType != "" && contact.AlertType != AlertTypeAll && contact.AlertType != alertType {
			logrus.Infof("skipping contact %s because category does not match %s", contact.Address, alertType)
			continue
		}

		// If we got here, the contact passed all filters
		notificationTargets[contact.Type] = append(notificationTargets[contact.Type], contact.Address)
	}

	return notificationTargets
}

// IntPtr returns a pointer to the given integer value
func IntPtr(i int) *int {
	return &i
}

// OperatorContact represents a single operator contact in YAML config
type OperatorContact struct {
	ID                string `yaml:"id"`
	SeverityThreshold *int   `yaml:"severity_threshold"`
	AlertType         string `yaml:"alert_type"`
	AlertName         string `yaml:"alert_name"`
}

// OperatorContactsConfig represents the YAML configuration file structure
type OperatorContactsConfig struct {
	Contacts struct {
		Email     []OperatorContact `yaml:"email"`
		Slack     []OperatorContact `yaml:"slack"`
		PagerDuty []OperatorContact `yaml:"pagerduty"`
	} `yaml:"contacts"`
}

// LoadClusterOperatorContacts loads cluster operator contacts from YAML config
func LoadClusterOperatorContacts(logger *logrus.Logger, configData []byte) ([]ContactInfo, error) {
	var config OperatorContactsConfig
	if err := yaml.Unmarshal(configData, &config); err != nil {
		logger.Errorf("error unmarshaling config: %v", err)
		return nil, fmt.Errorf("error unmarshaling config: %v", err)
	}

	logger.Infof("parsed contacts config: %+v", config)

	contacts := make([]ContactInfo, 0, len(config.Contacts.Email)+len(config.Contacts.Slack)+len(config.Contacts.PagerDuty))
	for _, email := range config.Contacts.Email {
		contacts = append(contacts, ContactInfo{
			Type:              ContactTypeEmail,
			Address:           email.ID,
			SeverityThreshold: email.SeverityThreshold,
			AlertName:         email.AlertName,
			AlertType:         AlertType(email.AlertType),
		})
	}
	for _, slack := range config.Contacts.Slack {
		contacts = append(contacts, ContactInfo{
			Type:              ContactTypeSlack,
			Address:           slack.ID,
			SeverityThreshold: slack.SeverityThreshold,
			AlertName:         slack.AlertName,
			AlertType:         AlertType(slack.AlertType),
		})
	}
	for _, pagerduty := range config.Contacts.PagerDuty {
		contacts = append(contacts, ContactInfo{
			Type:              ContactTypePagerDuty,
			Address:           pagerduty.ID,
			SeverityThreshold: pagerduty.SeverityThreshold,
			AlertName:         pagerduty.AlertName,
			AlertType:         AlertType(pagerduty.AlertType),
		})
	}
	logger.Infof("loaded %d notification targets", len(contacts))

	return contacts, nil
}
