// Example: observability/health-check-endpoint
//
// Demonstrates exposing KubeMQ health checks as an HTTP endpoint.
// The /healthz handler calls pub.HealthCheck(ctx) and sub.HealthCheck(ctx)
// to verify both publisher and subscriber connectivity to the broker.
// Returns HTTP 200 OK if both are healthy, or HTTP 503 Service Unavailable
// with error details if either check fails.
//
// See also: connection/health-check for the basic HealthCheck API without HTTP
//
// Channel: watermill-obs.health-endpoint
// Assumes KubeMQ is running on localhost:50000.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	kubemq "github.com/kubemq-io/watermill-kubemq/pkg/kubemq"
)

func main() {
	logger := watermill.NewStdLogger(false, false)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// --- Create publisher ---
	pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	// --- Create subscriber ---
	sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
		Address: "localhost:50000",
		Pattern: kubemq.PatternEvents,
		Logger:  logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	// --- Set up HTTP health check endpoint ---
	// The /healthz handler checks both publisher and subscriber connectivity.
	// This pattern is commonly used with Kubernetes liveness/readiness probes.
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		checkCtx, checkCancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer checkCancel()

		// Check publisher health
		if err := pub.HealthCheck(checkCtx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "publisher unhealthy: %v\n", err)
			return
		}

		// Check subscriber health
		if err := sub.HealthCheck(checkCtx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "subscriber unhealthy: %v\n", err)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})

	// --- Start HTTP server in background ---
	server := &http.Server{Addr: ":8088"}
	go func() {
		fmt.Println("Health check endpoint listening on :8088/healthz")
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Allow server to start
	time.Sleep(500 * time.Millisecond)

	// --- Test the health endpoint ---
	resp, err := http.Get("http://localhost:8088/healthz")
	if err != nil {
		log.Printf("Health check request failed: %v", err)
	} else {
		buf := make([]byte, 256)
		n, _ := resp.Body.Read(buf)
		resp.Body.Close()
		fmt.Printf("Health check response: status=%d, body=%s", resp.StatusCode, string(buf[:n]))
	}

	// --- Also demonstrate that the system works ---
	msgs, err := sub.Subscribe(ctx, "watermill-obs.health-endpoint")
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)

	msg := message.NewMessage(watermill.NewUUID(), []byte("health check test"))
	if err := pub.Publish("watermill-obs.health-endpoint", msg); err != nil {
		log.Fatal(err)
	}

	select {
	case received := <-msgs:
		fmt.Printf("Received: %s\n", string(received.Payload))
		received.Ack()
	case <-ctx.Done():
		log.Fatal("Timeout")
	}

	// --- Shutdown HTTP server ---
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	fmt.Println("Done! Health check endpoint demonstrated.")
}

// Expected output:
// Health check endpoint listening on :8088/healthz
// Health check response: status=200, body=ok
// Received: health check test
// Done! Health check endpoint demonstrated.
