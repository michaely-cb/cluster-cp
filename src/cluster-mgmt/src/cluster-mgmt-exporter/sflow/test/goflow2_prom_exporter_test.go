package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	PromExporterHttpPort  = 8080
	BinProtoInputFilename = "test_input_flow_samples_proto.bin"
	BinProtoMsgCnt        = 100
	FlowTimeout           = 2 * time.Second
)

func WaitForServer(port int, timeout time.Duration) bool {
	host := fmt.Sprintf(":%d", port)
	deadline := time.Now().Add(timeout)

	for {
		_, err := net.DialTimeout("tcp", host, 100*time.Millisecond)
		if err == nil {
			return true // server online
		}
		if time.Now().After(deadline) {
			// timeout occured, server likely offline
			return false
		}
	}
}

func ScrapeMetrics(t *testing.T, metricsURL string) []string {
	res, err := http.Get(metricsURL)
	if err != nil {
		t.Fatalf("%s", err)
	}
	assert.Equal(t, 200, res.StatusCode)
	prom_metrics_bytes, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("%s", err)
	}
	prom_metrics := strings.Split(string(prom_metrics_bytes), "\n")
	return prom_metrics
}

func TestExporter(t *testing.T) {

	// Start the exporter process.
	addr_arg := fmt.Sprintf("-addr=:%d", PromExporterHttpPort)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	exporter_cmd := exec.CommandContext(ctx, "/goflow2-prom-exporter", addr_arg, "-format=text", "-flowtimeout", FlowTimeout.String())
	stdin, err := exporter_cmd.StdinPipe()
	if err != nil {
		t.Fatalf("%s", err)
	}
	stdout, err := exporter_cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("%s", err)
	}
	stderr := new(strings.Builder)
	exporter_cmd.Stderr = stderr
	err = exporter_cmd.Start()
	if err != nil {
		t.Fatalf("%s", err)
	}

	// Read the binary proto messages from a file.
	protos_bin, err := os.ReadFile(BinProtoInputFilename)
	if err != nil {
		t.Fatalf("%s", err)
	}

	// Send the binary protos to the exporter process.
	n, err := stdin.Write(protos_bin)
	if err != nil {
		t.Fatalf("%s", err)
	} else if n != len(protos_bin) {
		t.Fatalf("Failed to write protos bin to exporter STDIN")
	}

	// Wait for the exporter to print out all the messages.
	scanner := bufio.NewScanner(stdout)
	scanner.Split(bufio.ScanLines)
	line_cnt := 0
	for scanner.Scan() {
		line_cnt += 1
		if line_cnt == BinProtoMsgCnt {
			break
		}
	}

	// Read the expected metrics from file.
	content, err := os.ReadFile("expected_metrics.prom")
	assert.NoError(t, err, "read expected_metrics.prom")
	expected_metrics := strings.Split(string(content), "\n")

	// Wait for the exporter's webserver to be up.
	ready := WaitForServer(PromExporterHttpPort, 5*time.Second)
	assert.True(t, ready, "Prom exporter HTTP server unavailable")

	metricsURL := fmt.Sprintf("http://localhost:%d/metrics", PromExporterHttpPort)
	prom_metrics := ScrapeMetrics(t, metricsURL)

	exp_samples_per_flow_cnt := 0
	exp_sampling_rate_cnt := 0
	for _, line := range expected_metrics {
		if strings.Contains(line, "sflow_samples_per_flow_total{") {
			exp_samples_per_flow_cnt += 1
		}
		if strings.Contains(line, "sflow_sampling_rate{") {
			exp_sampling_rate_cnt += 1
		}
		assert.Contains(t, prom_metrics, line)
	}

	samples_per_flow_cnt := 0
	sampling_rate_cnt := 0
	for _, line := range prom_metrics {
		if strings.Contains(line, "sflow_samples_per_flow_total{") {
			samples_per_flow_cnt += 1
		}
		if strings.Contains(line, "sflow_sampling_rate{") {
			sampling_rate_cnt += 1
		}
	}

	assert.Equal(t, exp_samples_per_flow_cnt, samples_per_flow_cnt)
	assert.Equal(t, exp_sampling_rate_cnt, sampling_rate_cnt)

	// Wait for the flows to time out, then scrape again.
	time.Sleep(FlowTimeout)
	prom_metrics = ScrapeMetrics(t, metricsURL)
	for _, line := range prom_metrics {
		assert.NotContains(t, line, "sflow_samples_per_flow_total", "All flow metrics should have expired")
	}

	// TODO: parse prom into JSON: https://github.com/prometheus/prom2json/blob/c99d38161f7263b6f8b42693e2963bf12deb2afd/cmd/prom2json/main.go#L79-L116
	// mfChan := make(chan *dto.MetricFamily, 1024)
	// err = prom2json.ParseReader(res.Body, mfChan)

	stdin.Close() // Should cause exporter to exit.
	assert.NoError(t, exporter_cmd.Wait(), "waiting for exporter SUT to exit. Stderr:", stderr.String())
	assert.True(t, exporter_cmd.ProcessState.Exited(), "exported SUT should have exited. Stderr:", stderr.String())
	if !exporter_cmd.ProcessState.Exited() {
		exporter_cmd.Process.Kill()
	}
}
