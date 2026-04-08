package kubemq

import (
	"net"
	"strconv"

	"github.com/ThreeDotsLabs/watermill/message"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// parseHost extracts the host from a "host:port" address string.
func parseHost(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return address
	}
	return host
}

// parsePort extracts the port from a "host:port" address string.
func parsePort(address string) int {
	_, portStr, err := net.SplitHostPort(address)
	if err != nil {
		return 50000
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 50000
	}
	return port
}

// injectTraceContext injects OTel trace context into the message metadata
// before marshaling, so it flows through to KubeMQ Tags.
func injectTraceContext(msg *message.Message) {
	otel.GetTextMapPropagator().Inject(
		msg.Context(),
		propagation.MapCarrier(msg.Metadata),
	)
}

// extractTraceContext extracts OTel trace context from message metadata.
func extractTraceContext(msg *message.Message) {
	ctx := otel.GetTextMapPropagator().Extract(
		msg.Context(),
		propagation.MapCarrier(msg.Metadata),
	)
	msg.SetContext(ctx)
}
