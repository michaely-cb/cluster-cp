package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	wscommon "cerebras.com/job-operator/common"
	pb "cerebras/pb/workflow/appliance/cluster_mgmt"
	commonpb "cerebras/pb/workflow/appliance/common"
)

type KafkaClient struct {
	bootstrapServer string
	secretDir       string
	dialer          *kafka.Dialer
}

func buildKafkaClient(clientset kubernetes.Interface) (KafkaClient, error) {
	client := KafkaClient{}

	ready, err := wscommon.IsServiceReady(clientset, "kafka", "kafka")
	if err != nil {
		if k8serrors.IsNotFound(err) {
			log.Info("kafka svc endpoints not deployed, skip building client")
			return client, nil
		}
		log.Errorf("kafka svc get error: %s", err)
		return client, err
	}
	if !ready {
		log.Error("kafka endpoints found but not ready likely due to kafka unhealthy, skip building client")
		return client, nil
	}

	client.bootstrapServer = os.Getenv("KAFKA_BOOTSTRAP_SERVER")
	if len(client.bootstrapServer) == 0 {
		errMsg := "kafka bootstrap server was not specified"
		log.Errorf(errMsg)
		return client, errors.New(errMsg)
	}

	client.secretDir = os.Getenv("KAFKA_SECRET_DIR")
	if len(client.secretDir) == 0 {
		errMsg := "kafka secret dir was not specified"
		log.Errorf(errMsg)
		return client, errors.New(errMsg)
	}

	dialer, err := newKafkaDialer(clientset, client.secretDir)
	if err != nil {
		log.Fatalf("Failed to instantiate a kafka dialer: %s", err)
		return client, err
	}
	client.dialer = dialer
	return client, err
}

func newKafkaDialer(clientset kubernetes.Interface, secretDir string) (*kafka.Dialer, error) {
	namespace := "kafka"
	secretName := "kafka-0-tls"

	secret, err := clientset.CoreV1().Secrets(namespace).Get(context.Background(), secretName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	// For each key in the secret data, decode the base64 encoded value and write to a file
	for filename, data := range secret.Data {
		err = ioutil.WriteFile(fmt.Sprintf("%s/%s", secretDir, filename), data, 0644)
		if err != nil {
			return nil, fmt.Errorf("error writing secret data to file: %v", err)
		}
	}

	caFile := fmt.Sprintf("%s/ca.crt", secretDir)
	certFile := fmt.Sprintf("%s/tls.crt", secretDir)
	keyFile := fmt.Sprintf("%s/tls.key", secretDir)

	// Load the CA cert
	caCert, err := ioutil.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Load the client cert and key
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{cert},
	}

	dialer := &kafka.Dialer{
		Timeout: 10 * time.Second,
		TLS:     tlsConfig,
	}

	return dialer, nil
}

func (server ClusterServer) assertKafkaDialer() error {
	if server.kafkaClient == nil {
		errMsg := "kafka dialer is not available"
		log.Errorf(errMsg)
		return errors.New(errMsg)
	}
	return nil
}

func (server ClusterServer) createTopicIfApplicable(jobId string, debugArgs *commonpb.DebugArgs) error {
	if server.kafkaClient == nil || debugArgs == nil || debugArgs.DebugMgr == nil ||
		debugArgs.DebugMgr.MessageBrokerStrategy != commonpb.DebugArgs_DebugMGR_MESSAGE_BROKER_STRATEGY_ENABLED {
		return nil
	}
	if err := server.assertKafkaDialer(); err != nil {
		log.Warnf("Skipped creating a topic for %s since kafka is not available", jobId)
		return nil
	}
	conn, err := server.kafkaClient.dialer.Dial("tcp", server.kafkaClient.bootstrapServer)
	if err != nil {
		log.Errorf("Unable to establish a connection to the kafka bootstrap server")
		return err
	}
	topicConfig := kafka.TopicConfig{
		Topic:             jobId,
		ReplicationFactor: 1,
		NumPartitions:     1,
	}
	err = conn.CreateTopics(topicConfig)
	if err != nil {
		log.Errorf("Failed to create a topic for %s: %v", jobId, err)
		return err
	}
	defer conn.Close()
	log.Infof("successfully created topic %s", jobId)

	w := &kafka.Writer{
		Addr: kafka.TCP(server.kafkaClient.bootstrapServer),
		Transport: &kafka.Transport{
			Dial:        server.kafkaClient.dialer.DialFunc,
			DialTimeout: server.kafkaClient.dialer.Timeout,
			TLS:         server.kafkaClient.dialer.TLS,
		},
		Topic: jobId,
	}

	// sanity check by publishing the first message
	message := fmt.Sprintf("Topic %s created at $%s", jobId, time.Now().UTC().Format(time.RFC3339))
	err = w.WriteMessages(context.Background(), kafka.Message{Value: []byte(message)})
	if err != nil {
		log.Errorf("Error occurred during writing the inital message for '%s'", jobId)
		return err
	}

	return nil
}

func (server ClusterServer) IsMessageBrokerAvailable(_ context.Context, _ *pb.MessageBrokerAvailabilityCheckRequest) (
	*pb.MessageBrokerAvailabilityCheckResponse, error) {
	return &pb.MessageBrokerAvailabilityCheckResponse{
		IsAvailable: server.kafkaClient != nil,
	}, nil
}

func (server ClusterServer) PublishMessages(stream pb.ClusterManagement_PublishMessagesServer) (err error) {
	if err = server.assertKafkaDialer(); err != nil {
		log.Warn("Skipped publishing messages since kafka is not available")
		return nil
	}

	w := &kafka.Writer{
		Addr: kafka.TCP(server.kafkaClient.bootstrapServer),
		Transport: &kafka.Transport{
			Dial:        server.kafkaClient.dialer.DialFunc,
			DialTimeout: server.kafkaClient.dialer.Timeout,
			TLS:         server.kafkaClient.dialer.TLS,
		},
	}

	defer func() {
		if w != nil {
			errClose := w.Close()
			if errClose != nil {
				log.Errorf("Error occurred during closing message broker writer for '%s': %s", w.Topic, errClose)
			} else {
				log.Infof("Successfully closed message broker writer for '%s'", w.Topic)
			}
		} else {
			log.Infof("Message broker writer for '%s' was not created hence skipping the cleanup.", w.Topic)
		}
		if err != nil {
			log.Errorf("Error occurred during publishing messages for '%s': %s", w.Topic, err.Error())
			err = grpcStatus.Error(codes.Internal, err.Error())
		} else {
			log.Infof("Successfully completed publish messages call for '%s'", w.Topic)
		}
	}()

	// Receive the first message which includes the JobId
	in, err := stream.Recv()
	if err != nil {
		log.Errorf("Error occurred during receiving the job ID: %v", err)
		return err
	}

	w.Topic = in.JobId
	log.Infof("Producer stream is ready for '%s'", w.Topic)

	kafkaCtx, cancelKafkaCtx := context.WithCancel(context.Background())
	defer cancelKafkaCtx()
	for {
		select {
		case <-stream.Context().Done():
			// Client has disconnected abruptly
			cancelKafkaCtx()
			log.Warnf("Client has disconnected abruptly from publishing messages for '%s'", w.Topic)
			return nil
		default:
			in, err = stream.Recv()
			if err == io.EOF {
				// Client has closed the connection, we can return nil
				log.Infof("Client successfully closed the producer stream for job '%s'", w.Topic)
				return nil
			}
			if err != nil {
				// Another kind of error occurred, return it
				log.Errorf("Error occurred during receiving messages from the producer stream for job '%s'", w.Topic)
				return err
			}
			err = w.WriteMessages(kafkaCtx, kafka.Message{Value: []byte(in.Message)})
			if err != nil {
				log.Errorf("Error occurred during writing messages to the message broker for job '%s'", w.Topic)
				err = grpcStatus.Error(codes.Internal, err.Error())
				return err
			}
		}
	}
}

func (server ClusterServer) SubscribeMessages(in *pb.SubscribeMessagesRequest, stream pb.ClusterManagement_SubscribeMessagesServer) (err error) {
	topic := in.JobId
	if err = server.assertKafkaDialer(); err != nil {
		log.Warnf("Skipped subscribing messages for topic %s since kafka is not available", topic)
		return nil
	}
	log.Infof("Subscribing messages for %s", topic)

	readConfig := kafka.ReaderConfig{
		Brokers: []string{server.kafkaClient.bootstrapServer},
		Dialer:  server.kafkaClient.dialer,
		Topic:   topic,
	}
	// Only used to facilitate testing
	if in.FromBeginning {
		readConfig.StartOffset = kafka.FirstOffset
	}
	r := kafka.NewReader(readConfig)

	defer func() {
		if r != nil {
			errClose := r.Close()
			if errClose != nil {
				log.Errorf("Error occurred during closing message broker reader for '%s': %s", topic, errClose.Error())
			} else {
				log.Infof("Successfully closed message broker reader for '%s'", topic)
			}
		} else {
			log.Infof("Message broker reader for '%s' was not created hence skipping the cleanup.", topic)
		}
		if err != nil {
			log.Errorf("Error occurred during subscribing messages for '%s': %s", topic, err.Error())
			err = grpcStatus.Error(codes.Internal, err.Error())
		} else {
			log.Infof("Successfully completed subscribing messages call for '%s'", topic)
		}
	}()

	for {
		select {
		case <-stream.Context().Done():
			// Client has disconnected
			log.Infof("Client has disconnected from receiving messages for '%s'", topic)
			return nil
		default:
			m, err := r.ReadMessage(stream.Context())
			if err != nil {
				if errors.Is(err, context.Canceled) {
					// The context was canceled, likely because the client disconnected
					continue
				}
				log.Errorf("Error occurred during reading messages from the message broker for '%s': %v", topic, err)
				err = grpcStatus.Error(codes.Internal, err.Error())
				return err
			}
			if err := stream.Send(&pb.SubscribeMessagesResponse{Message: string(m.Value)}); err != nil {
				log.Errorf("Error occurred during relaying messages to the client for '%s': %v", topic, err)
				return err
			}
		}
	}
}
