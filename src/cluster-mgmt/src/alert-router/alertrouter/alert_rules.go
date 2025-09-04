package alertrouter

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

// AlertRuleMapping represents a single alert rule mapping in YAML config
type AlertRuleMapping struct {
	Code     string    `yaml:"code"`     // "NodeNotPingable" etc.
	Category AlertType `yaml:"category"` // "cluster" or "job"
	Severity int       `yaml:"severity"` // 1-3
}

// AlertRuleMappings manages the mappings from alert codes to categories
type AlertRuleMappings struct {
	mappings map[string]AlertRuleMapping
}

// NewAlertRuleMappings creates a new AlertRuleMappings instance
func NewAlertRuleMappings() *AlertRuleMappings {
	return &AlertRuleMappings{
		mappings: make(map[string]AlertRuleMapping),
	}
}

// GetMapping returns the mapping for a given alert code
func (a *AlertRuleMappings) GetMapping(alertCode string) (AlertRuleMapping, bool) {
	mapping, ok := a.mappings[alertCode]
	return mapping, ok
}

func (a *AlertRuleMappings) GetDefaultMapping(name string) AlertRuleMapping {
	return AlertRuleMapping{
		Code:     fmt.Sprintf("default-%s", name),
		Category: AlertTypeCluster,
		Severity: 3,
	}
}

// SetMappingsFromConfig updates the alert rule mappings from raw YAML config
func (a *AlertRuleMappings) SetMappingsFromConfig(logger *logrus.Logger, configData []byte) error {
	var config map[string][]interface{}
	if err := yaml.Unmarshal(configData, &config); err != nil {
		logger.WithFields(logrus.Fields{
			"event": "error_unmarshaling_config",
			"error": err,
		}).Errorf("error unmarshaling config")
		return fmt.Errorf("error unmarshaling config: %v", err)
	}

	logger.WithFields(logrus.Fields{
		"event":  "parsed_config",
		"config": config,
	}).Info("parsed config")

	a.mappings = make(map[string]AlertRuleMapping)
	for code, value := range config {
		if len(value) != 2 {
			logger.WithFields(logrus.Fields{
				"event":                "skipping_invalid_entry",
				"alert_code":           code,
				"alert_classification": value,
			}).Warn("skipping invalid entry in alert rule config: wrong number of elements")
			continue
		}
		validValues := false
		if category, ok := value[0].(string); ok {
			if severity, ok := value[1].(int); ok {
				a.mappings[code] = AlertRuleMapping{
					Code:     code,
					Category: AlertType(category),
					Severity: severity,
				}
				validValues = true
			}
		}
		if !validValues {
			logger.WithFields(logrus.Fields{
				"event":                "skipping_invalid_entry",
				"alert_code":           code,
				"alert_classification": value,
			}).Warn("skipping invalid entry in alert rule config: expect string category and int severity")
		}
	}
	return nil
}

// GetMappings returns the current alert rule mappings
func (a *AlertRuleMappings) GetMappings() map[string]AlertRuleMapping {
	return a.mappings
}
