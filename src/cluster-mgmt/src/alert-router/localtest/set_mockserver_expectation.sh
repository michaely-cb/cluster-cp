#!/bin/bash

NAMESPACE="prometheus"
SERVICE_NAME="mockserver"
PORT=3180

# MockServer Reference: https://www.mock-server.com/

# Step 1: Port-forward MockServer
echo
echo "Starting port-forwarding for MockServer..."
kubectl -n "$NAMESPACE" port-forward service/"$SERVICE_NAME" "$PORT" > /dev/null 2>&1 &
PF_PID=$!  # Get the process ID of the port-forward command
sleep 2  # Allow time for the port-forward to establish

# Step 2: Send a PUT request to MockServer to create an expectation
echo
echo "Setting up MockServer expectation..."
curl -X PUT "http://localhost:$PORT/mockserver/expectation" \
  -H "Content-Type: application/json" \
  -d '{
        "httpRequest": {
          "method": "POST",
          "path": "/slack-webhook"
        },
        "httpResponse": {
          "statusCode": 200,
          "body": "OK",
          "delay": {
            "timeUnit": "MILLISECONDS",
            "value": 10
          }
        },
        "times": {
          "unlimited": true
        }
      }'

# Step 3: Stop port-forwarding
echo
echo "Stopping port-forwarding..."
kill $PF_PID

echo
echo "MockServer expectation set up"

### P.S. Example of setting up an error response:
# curl -X PUT "http://localhost:1080/mockserver/expectation" \
# -H "Content-Type: application/json" \
# -d '{
#   "httpRequest": {
#     "method": "GET",
#     "path": "/slack-webhook"
#   },
#   "httpResponse": {
#     "statusCode": 500,
#     "body": "{ \"error\": \"Internal Server Error\" }",
#     "headers": [
#       { "name": "Content-Type", "values": ["application/json"] }
#     ]
#   }
# }'