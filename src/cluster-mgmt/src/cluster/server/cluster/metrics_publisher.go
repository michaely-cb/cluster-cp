package cluster

import (
	"encoding/json"

	log "github.com/sirupsen/logrus"

	wscommon "cerebras.com/job-operator/common"
)

type MetricsPublisher interface {
	TrackMetric(
		name string,
		value float64,
		timestampSec uint64,
		wsjob string,
		labels map[string]string,
	)
}

type LogMetricsPublisher struct{}

// Tracks the metric by writing a log line with the metric in json format.
// If everything is working correctly, the log line will be scraped by Fluentbit and sent
// to Loki (which can be queried from Grafana). See https://github.com/Cerebras/monolith/pull/107626
func (metricsPublisher LogMetricsPublisher) TrackMetric(
	name string,
	value float64,
	timestampSec uint64,
	wsjob string,
	labels map[string]string,
) {
	outputMap := map[string]interface{}{
		"metric":       name,
		"value":        value,
		"timestampSec": timestampSec,
		"labels":       labels,
	}
	outputString, err := json.Marshal(outputMap)
	if err != nil {
		log.Errorf("Error parsing reported metric: %v, %v", outputMap, err)
		return
	}

	logFields := map[string]interface{}{
		// Adding time_override_epoch_sec overrides the time of the log line
		// Otherwise, the time would just be the moment that this log line is printed
		"time_override_epoch_sec": timestampSec,
		"wsjob":                   wsjob,
		"namespace":               wscommon.Namespace,
	}
	log.WithFields(logFields).Info(string(outputString))
}

func NewLogMetricsPublisher() MetricsPublisher {
	return &LogMetricsPublisher{}
}
