package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var (
	tlsCert string
	tlsKey  string
	port    int
	codecs  = serializer.NewCodecFactory(runtime.NewScheme())
	logger  = log.New(os.Stdout, "http: ", log.LstdFlags)
)

var rootCmd = &cobra.Command{
	Use:   "kube-webhook",
	Short: "Kubernetes mutating webhook",
	Long: `A basic mutating webhook for kubernetes.

Example:
$ kube-webhook --tls-cert <tls_cert> --tls-key <tls_key> --port <port>`,
	Run: func(cmd *cobra.Command, args []string) {
		if tlsCert == "" || tlsKey == "" {
			fmt.Println("--tls-cert and --tls-key required")
			os.Exit(1)
		}
		runWebhookServer(tlsCert, tlsKey)
	},
}

func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

func init() {
	rootCmd.Flags().StringVar(&tlsCert, "tls-cert", "", "Certificate for TLS")
	rootCmd.Flags().StringVar(&tlsKey, "tls-key", "", "Private key file for TLS")
	rootCmd.Flags().IntVar(&port, "port", 443, "Port to listen on for HTTPS traffic")
}

func admissionReviewFromRequest(r *http.Request, deserializer runtime.Decoder) (*admissionv1.AdmissionReview, error) {
	if r.Header.Get("Content-Type") != "application/json" {
		return nil, fmt.Errorf("expected application/json content-type")
	}

	var body []byte
	if r.Body != nil {
		requestData, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		body = requestData
	}

	admissionReviewRequest := &admissionv1.AdmissionReview{}
	if _, _, err := deserializer.Decode(body, nil, admissionReviewRequest); err != nil {
		return nil, err
	}

	return admissionReviewRequest, nil
}

func writeDecodeError(w http.ResponseWriter, err error) {
	msg := fmt.Sprintf("error decoding raw request: %v", err)
	logger.Printf(msg)
	w.WriteHeader(500)
	w.Write([]byte(msg))
}

func registerHandlers(clientset *kubernetes.Clientset, dynClient dynamic.Interface) {
	http.HandleFunc("/adjust-resource", adjustResource)
	http.HandleFunc("/system-deploy-admission", func(w http.ResponseWriter, r *http.Request) {
		deployAdmission(w, r, clientset, dynClient)
	})
	http.HandleFunc("/mutate-ceph-multus", mutateCephMultusPod)
	http.HandleFunc("/schedule-kafka-replicas", func(w http.ResponseWriter, r *http.Request) {
		scheduleKafkaStatefulSetPod(w, r, clientset)
	})
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	http.HandleFunc("/add-ingress-annotation", addIngressAnnotation)
}

func runWebhookServer(certFile, keyFile string) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		panic(err)
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		panic(fmt.Sprintf("error getting in-cluster config: %v", err))
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(fmt.Sprintf("error creating dynamic client: %v", err))
	}
	dynClient, err := dynamic.NewForConfig(config)
	if err != nil {
		panic(fmt.Sprintf("error creating clientset: %v", err))
	}
	registerHandlers(clientset, dynClient)

	logger.Println("Starting webhook server")
	server := http.Server{
		Addr: fmt.Sprintf(":%d", port),
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
		},
		ErrorLog:     logger,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  15 * time.Second,
	}

	go func() {
		if err := server.ListenAndServeTLS("", ""); err != nil {
			logger.Printf("ListenAndServe error: %v\n", err)
		}
	}()

	// Listen for SIGINT and SIGTERM signals and gracefully shut down the server when they are received
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		logger.Printf("Server shutdown error: %v\n", err)
	}
}
