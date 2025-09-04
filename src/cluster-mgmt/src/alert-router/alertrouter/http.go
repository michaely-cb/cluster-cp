package alertrouter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// HTTPClient defines the interface for making HTTP requests
type HTTPClient interface {
	PostJSON(url string, payload interface{}) error
}

// httpClient implements HTTPClient using the standard http.Client
type httpClient struct {
	client *http.Client
}

// NewHTTPClient creates a new HTTPClient with reasonable defaults
func NewHTTPClient() HTTPClient {
	return &httpClient{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// PostJSON sends a POST request with JSON payload to the specified URL
func (c *httpClient) PostJSON(url string, payload interface{}) error {
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON payload: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("request failed with status code %d", resp.StatusCode)
	}

	return nil
}
