package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/netip"
	"os"
	"strings"
	"sync"
	"time"

	flowmessage "github.com/netsampler/goflow2/v2/pb"

	// import various formatters
	"github.com/netsampler/goflow2/v2/format"
	_ "github.com/netsampler/goflow2/v2/format/binary"
	_ "github.com/netsampler/goflow2/v2/format/json"
	_ "github.com/netsampler/goflow2/v2/format/text"

	// import various transports
	"github.com/netsampler/goflow2/v2/transport"
	_ "github.com/netsampler/goflow2/v2/transport/file"
	_ "github.com/netsampler/goflow2/v2/transport/kafka"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/protobuf/encoding/protodelim"
)

var (
	version    = ""
	buildinfos = ""
	AppVersion = "Enricher " + version + " " + buildinfos

	LogLevel = flag.String("loglevel", "info", "Log level")
	LogFmt   = flag.String("logfmt", "normal", "Log formatter")

	Format    = flag.String("format", "json", fmt.Sprintf("Choose the format (available: %s)", strings.Join(format.GetFormats(), ", ")))
	Transport = flag.String("transport", "file", fmt.Sprintf("Choose the transport (available: %s)", strings.Join(transport.GetTransports(), ", ")))
	NoOutput  = flag.Bool("noout", false, "Do not output samples (only export Prometheus metrics)")

	Addr = flag.String("addr", ":8080", "HTTP server address (Prometheus exporter)")

	Version = flag.Bool("v", false, "Print version")

	FlowTimeout = flag.String("flowtimeout", "10m", "Remove the flow if no packet samples are received within the timeout")

	FlowLatestSampleTime = sync.Map{}
	FlowSampleCnt        = sync.Map{}
	AgentSamplingRate    = sync.Map{}

	PromNamespace = "sflow"
)

type FlowFiveTuple struct {
	saddr [4]byte
	daddr [4]byte
	proto uint8
	sport uint16
	dport uint16
}

type FlowSampleLocation struct {
	Flow      FlowFiveTuple
	EgrPort   uint32
	AgentAddr [4]byte
}

// Collector pattern based on: https://betterstack.com/community/guides/monitoring/prometheus-exporter/
type metrics struct {
	SamplesPerFlowDesc *prometheus.Desc
	SamplingRateDesc   *prometheus.Desc
}

func NewMetrics(namespace string) *metrics {
	return &metrics{
		SamplesPerFlowDesc: prometheus.NewDesc(
			namespace+"_samples_per_flow_total",
			"Samples per labeled flow.",
			[]string{"agent", "saddr", "daddr", "proto", "sport", "dport", "portindex"},
			nil,
		),
		SamplingRateDesc: prometheus.NewDesc(
			namespace+"_sampling_rate",
			"Sampling rate for labeled agent.",
			[]string{"agent"},
			nil,
		),
	}
}

type CollectMetrics struct {
	metrics *metrics
}

func NewCollector(namespace string, reg prometheus.Registerer) *CollectMetrics {
	m := NewMetrics(namespace)
	c := &CollectMetrics{metrics: m}
	reg.MustRegister(c)
	return c
}

func (c *CollectMetrics) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.metrics.SamplesPerFlowDesc
	ch <- c.metrics.SamplingRateDesc
}

func (c *CollectMetrics) Collect(ch chan<- prometheus.Metric) {
	timeout, err := time.ParseDuration(*FlowTimeout)
	if err != nil {
		panic("Bad format for -flowtimeout")
	}
	expiredFlows := make(map[FlowSampleLocation]bool)
	FlowSampleCnt.Range(func(k interface{}, v interface{}) bool {
		flow := k.(FlowSampleLocation)
		count := v.(int)
		val, ok := FlowLatestSampleTime.Load(flow.Flow)
		if ok {
			lastTime := val.(time.Time)
			if time.Since(lastTime) > timeout { // Timeout expired.
				expiredFlows[flow] = true
				// Don't create a metric for this flow.
				return true // Continue.
			}
		}
		saddr := netip.AddrFrom4(flow.Flow.saddr).Unmap().String()
		daddr := netip.AddrFrom4(flow.Flow.daddr).Unmap().String()
		agentStr := netip.AddrFrom4(flow.AgentAddr).String()

		ch <- prometheus.MustNewConstMetric(c.metrics.SamplesPerFlowDesc,
			prometheus.CounterValue, float64(count), agentStr, saddr, daddr, fmt.Sprint(flow.Flow.proto), fmt.Sprint(flow.Flow.sport), fmt.Sprint(flow.Flow.dport), fmt.Sprint(flow.EgrPort))
		return true
	})

	AgentSamplingRate.Range(func(k interface{}, v interface{}) bool {
		agentStr := netip.AddrFrom4(k.([4]byte)).String()
		ch <- prometheus.MustNewConstMetric(c.metrics.SamplingRateDesc, prometheus.GaugeValue, float64(v.(uint64)), agentStr)
		return true
	})

	// Remove timed out flows.
	for flow, _ := range expiredFlows {
		FlowSampleCnt.Delete(flow)
		FlowLatestSampleTime.Delete(flow.Flow)
	}

}

func RecordPacketSample(msg *ProtoProducerMessage) {
	if msg.OutIf == 0 {
		// Ignore packets sampled on ingress.
		return
	}
	now := time.Now()
	// TODO: handle IPv6 addresses.
	flow_id := FlowFiveTuple{
		[4]byte(msg.SrcAddr),
		[4]byte(msg.DstAddr),
		uint8(msg.Proto),
		uint16(msg.SrcPort),
		uint16(msg.DstPort)}

	sample_loc := FlowSampleLocation{flow_id, msg.OutIf, [4]byte(msg.SamplerAddress)}
	sample_cnt, found := FlowSampleCnt.Load(sample_loc)
	if !found {
		sample_cnt = 0
	}
	FlowSampleCnt.Store(sample_loc, sample_cnt.(int)+1)
	FlowLatestSampleTime.Store(flow_id, now)

	if msg.SamplingRate != 0 {
		samplerAddr := [4]byte(msg.SamplerAddress)
		samplingRateChanged := true
		if oldSamplingRate, ok := AgentSamplingRate.Load(samplerAddr); ok {
			samplingRateChanged = oldSamplingRate != msg.SamplingRate
		}
		if samplingRateChanged {
			AgentSamplingRate.Store(samplerAddr, msg.SamplingRate)
		}
	}
}

type ProtoProducerMessage struct {
	flowmessage.FlowMessage
}

func (m *ProtoProducerMessage) MarshalBinary() ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	_, err := protodelim.MarshalTo(buf, m)
	return buf.Bytes(), err
}

func main() {
	flag.Parse()

	if *Version {
		fmt.Println(AppVersion)
		os.Exit(0)
	}

	var loglevel slog.Level
	if err := loglevel.UnmarshalText([]byte(*LogLevel)); err != nil {
		log.Fatal("error parsing log level")
	}

	lo := slog.HandlerOptions{
		Level: loglevel,
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &lo))

	switch *LogFmt {
	case "json":
		logger = slog.New(slog.NewJSONHandler(os.Stderr, &lo))
	}

	slog.SetDefault(logger)

	var err error
	formatter, err := format.FindFormat(*Format)
	if err != nil {
		log.Fatal(err)
	}

	transporter, err := transport.FindTransport(*Transport)
	if err != nil {
		slog.Error("error transporter", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer transporter.Close()

	wg := &sync.WaitGroup{}

	reg := prometheus.NewRegistry()

	NewCollector("sflow", reg)

	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})

	http.Handle("/metrics", handler)
	http.HandleFunc("/__health", func(wr http.ResponseWriter, r *http.Request) {
		wr.WriteHeader(http.StatusOK)
		if _, err := wr.Write([]byte("OK\n")); err != nil {
			slog.Error("error writing HTTP", slog.String("error", err.Error()))

		}
	})
	http.HandleFunc("/__clear", func(wr http.ResponseWriter, r *http.Request) {
		var clearMap = func(m *sync.Map) {
			m.Range(func(k interface{}, v interface{}) bool {
				m.Delete(k)
				return true
			})
		}
		clearMap(&AgentSamplingRate)
		clearMap(&FlowSampleCnt)
		clearMap(&FlowLatestSampleTime)

		wr.WriteHeader(http.StatusOK)
		if _, err := wr.Write([]byte("Metrics cleared.\n")); err != nil {
			slog.Error("error writing HTTP", slog.String("error", err.Error()))

		}
	})
	srv := http.Server{
		Addr:              *Addr,
		ReadHeaderTimeout: time.Second * 5,
	}
	if *Addr != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			logger := logger.With(slog.String("http", *Addr))
			err := srv.ListenAndServe()
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("HTTP server error", slog.String("error", err.Error()))
				os.Exit(1)
			}
			logger.Info("exporter closed HTTP server")
		}()
	}

	logAttr := []any{
		slog.String("addr", *Addr),
		slog.String("flowtimeout", *FlowTimeout),
	}
	logger = logger.With(logAttr...)
	logger.Info("Starting Cluster Mgmt's sFlow Prometheus exporter")

	rdr := bufio.NewReader(os.Stdin)

	var msg ProtoProducerMessage
	for {
		if err := protodelim.UnmarshalFrom(rdr, &msg); err != nil && errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			slog.Error("error unmarshalling message", slog.String("error", err.Error()))
			continue
		}

		// TODO: process the message on one or more threads.
		RecordPacketSample(&msg)

		if !*NoOutput {
			key, data, err := formatter.Format(&msg)
			if err != nil {
				slog.Error("error formatting message", slog.String("error", err.Error()))
				continue
			}

			err = transporter.Send(key, data)
			if err != nil {
				slog.Error("error sending message", slog.String("error", err.Error()))
				continue
			}
		}

		msg.Reset()
	}

	// TODO: catch exit signal (not just EOF).

	logger.Info("Stopping Cluster Mgmt's sFlow Prometheus exporter")

	// close http server (prometheus + health check)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("error shutting-down HTTP server", slog.String("error", err.Error()))
	}
	cancel()

	wg.Wait()
}
