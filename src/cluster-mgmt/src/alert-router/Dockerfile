# Accept a build argument for the builder’s base image
ARG BASE_IMAGE=golang:1.21
FROM ${BASE_IMAGE} AS builder

# Set the Current Working Directory inside the container
WORKDIR /alert-router

# Ensure pb/ and job-operator/ is available for vendoring
COPY pb/ /pb/
COPY job-operator/ /job-operator/
COPY alert-router/ /alert-router/

# Vendor dependencies
RUN go mod tidy && go mod vendor

# Build the Go app
RUN CGO_ENABLED=0 go build -mod=vendor -o /app/alert-router && chmod +x /alert-router

# Runtime stage using go-runner
FROM registry.k8s.io/build-image/go-runner:v2.3.1-go1.21.8-bullseye.0

WORKDIR /

# Copy the pre-built binary file from the previous stage (it's already executable)
COPY --from=builder /app/alert-router /alert-router

# Expose port 8080 to the outside world
EXPOSE 8080

# Set non-root user
USER 65532:65532

# Command to run the executable with go-runner
ENTRYPOINT ["/go-runner"]
CMD ["--redirect-stderr=true", "--also-stdout=true", "--", "/alert-router"]
