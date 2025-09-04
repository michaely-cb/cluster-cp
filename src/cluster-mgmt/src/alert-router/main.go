package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"cerebras.com/alert-router/alertrouter"
	"github.com/sirupsen/logrus"
	mail "github.com/wneessen/go-mail"
)

var (
	port = flag.Int("port", 8080, "Port to listen on")
)


// readSMTPCredentialsFromSecret reads SMTP credentials from mounted secret files
func readSMTPCredentialsFromSecret(logger *logrus.Logger) (username, password string, err error) {
	// Read username from secret file
	usernameBytes, err := os.ReadFile("/etc/smtp-credentials/username")
	if err != nil {
		return "", "", fmt.Errorf("failed to read SMTP username from secret: %v", err)
	}
	username = strings.TrimSpace(string(usernameBytes))
	
	// Read password from secret file
	passwordBytes, err := os.ReadFile("/etc/smtp-credentials/password")
	if err != nil {
		return "", "", fmt.Errorf("failed to read SMTP password from secret: %v", err)
	}
	password = strings.TrimSpace(string(passwordBytes))
	
	if username == "" || password == "" {
		return "", "", fmt.Errorf("SMTP credentials are empty in secret files")
	}
	
	logger.WithFields(logrus.Fields{
		"username": username,
		"password_empty": password == "",
		"has_password": password != "",
	}).Info("Successfully loaded SMTP credentials from Kubernetes secret")
	return username, password, nil
}

func main() {
	flag.Parse()
	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.DebugLevel)
	operatorContactsPath := os.Getenv("OPERATOR_CONTACTS_PATH")
	alertRuleMappingsPath := os.Getenv("ALERT_RULE_MAPPINGS_PATH")
	smtpHost := os.Getenv("SMTP_HOST")
	smtpPortStr := os.Getenv("SMTP_PORT")
	smtpInsecure := os.Getenv("SMTP_INSECURE") == "true"
	clusterName := os.Getenv("CLUSTER_NAME")
	clusterDomainName := os.Getenv("CLUSTER_DOMAIN_NAME")
	
	// Try to read SMTP credentials from Kubernetes secret first
	smtpUsername, smtpPassword, err := readSMTPCredentialsFromSecret(logger)
	if err != nil {
		// Fall back to environment variables
		logger.WithError(err).Warn("Failed to read SMTP credentials from Kubernetes secret, falling back to environment variables")
		smtpUsername = os.Getenv("SMTP_USERNAME")
		smtpPassword = os.Getenv("SMTP_PASSWORD")
		
		// Log credential status from environment variables
		logger.WithFields(logrus.Fields{
			"username": smtpUsername,
			"password_empty": smtpPassword == "",
			"has_password": smtpPassword != "",
			"source": "environment_variables",
		}).Info("Using SMTP credentials from environment variables")
	}
	smtpPort, err := strconv.Atoi(smtpPortStr)
	if err != nil {
		log.Fatalf("Failed to convert SMTP port to integer: %v", err)
	}

	// Skip authentication for test environments (localhost, mailhog)
	isTestEnvironment := smtpHost == "localhost" || smtpHost == "mailhog"
	if !isTestEnvironment && (smtpUsername == "" || smtpPassword == "") {
		log.Fatalf("SMTP_USERNAME and SMTP_PASSWORD must be set for non-test SMTP hosts")
	}

	if smtpHost == "" || smtpPort == 0 || clusterName == "" || clusterDomainName == "" ||
		operatorContactsPath == "" || alertRuleMappingsPath == "" {
		fmt.Printf("Got SMTP_HOST: %s\n", smtpHost)
		fmt.Printf("Got SMTP_PORT: %d\n", smtpPort)
		fmt.Printf("Got CLUSTER_NAME: %s\n", clusterName)
		fmt.Printf("Got CLUSTER_DOMAIN_NAME: %s\n", clusterDomainName)
		fmt.Printf("Got SMTP_USERNAME: %s\n", smtpUsername)
		fmt.Printf("Got SMTP_PASSWORD: %s\n", smtpPassword)
		fmt.Printf("Got SMTP_INSECURE: %v\n", smtpInsecure)
		fmt.Printf("Got OPERATOR_CONTACTS_PATH: %s\n", operatorContactsPath)
		fmt.Printf("Got ALERT_RULE_MAPPINGS_PATH: %s\n", alertRuleMappingsPath)
		fmt.Println("Required environment variables not set properly")
		os.Exit(1)
	}

	// Create a new mail client
	mailClientOpts := []mail.Option{
		mail.WithPort(smtpPort),
	}

	if !smtpInsecure {
		mailClientOpts = append(mailClientOpts,
			mail.WithSMTPAuth(mail.SMTPAuthPlain),
			mail.WithUsername(smtpUsername),
			mail.WithPassword(smtpPassword),
		)
	} else {
		mailClientOpts = append(mailClientOpts,
			mail.WithTLSPolicy(mail.NoTLS),
		)
	}

	mailClient, err := mail.NewClient(
		smtpHost,
		mailClientOpts...,
	)
	if err != nil {
		log.Fatalf("Failed to create mail client: %v", err)
	}
	sender := fmt.Sprintf("alertmanager-%s@cerebras.net", clusterName)

	mailSender := alertrouter.NewMailSender(mailClient, sender)
	alertRules := alertrouter.NewAlertRuleMappings()

	// Initialize k8s client for querying WSJobs
	jobContactInfoRetriever, err := alertrouter.NewWSClientContactInfoRetriever()
	if err != nil {
		log.Fatalf("Failed to create k8s client: %v", err)
	}

	// Create alert handler
	alertHandler, err := alertrouter.NewAlertHandler(logger, clusterDomainName, mailSender, jobContactInfoRetriever, alertRules)
	if err != nil {
		log.Fatalf("Failed to create alert handler: %v", err)
	}
	defer alertHandler.StopWorkers()

	// Create a single FileWatcher for both configs
	fileWatcher, err := alertrouter.NewFileWatcher(logger)
	if err != nil {
		log.Fatalf("Failed to create file watcher: %v", err)
	}

	// Add watch for operator contacts
	if err := fileWatcher.AddWatch(operatorContactsPath, func(data []byte) error {
		if err := alertHandler.UpdateClusterOperatorsFromConfig(data); err != nil {
			return fmt.Errorf("failed to parse config: %v", err)
		}
		return nil
	}); err != nil {
		log.Fatalf("Failed to add operator contacts watch: %v", err)
	}

	// Add watch for alert rule mappings
	if err := fileWatcher.AddWatch(alertRuleMappingsPath, func(data []byte) error {
		if err := alertRules.SetMappingsFromConfig(logger, data); err != nil {
			return fmt.Errorf("failed to parse config: %v", err)
		}
		logger.WithField("config", alertRules.GetMappings()).Info("Updated alert rule mappings")
		return nil
	}); err != nil {
		log.Fatalf("Failed to add alert rule mappings watch: %v", err)
	}

	// Start the file watcher
	if err := fileWatcher.Start(); err != nil {
		log.Fatalf("Failed to start file watcher: %v", err)
	}
	defer fileWatcher.Stop()

	http.HandleFunc("/webhook", alertHandler.HandleWebhook)
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
		w.WriteHeader(http.StatusOK)
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		status := alertHandler.GetStats().GetStatusResponse(alertRules, alertHandler.GetClusterOperators())
		if err := json.NewEncoder(w).Encode(status); err != nil {
			logger.WithError(err).Error("Failed to encode status response")
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
	})

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("Starting server on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
