# Watermill KubeMQ

[![CI](https://github.com/kubemq-io/watermill-kubemq/actions/workflows/ci.yml/badge.svg)](https://github.com/kubemq-io/watermill-kubemq/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/kubemq-io/watermill-kubemq.svg)](https://pkg.go.dev/github.com/kubemq-io/watermill-kubemq/pkg/kubemq)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Production-ready [Watermill](https://watermill.io/) pub/sub plugin for [KubeMQ](https://kubemq.io/) message broker. Provides Watermill `message.Publisher` and `message.Subscriber` interfaces across three KubeMQ messaging patterns plus a native `CQPublisher` for Commands and Queries.

## Features

- Three messaging patterns: **Events** (fire-and-forget), **EventsStore** (persistent), **Queues** (reliable with ack/nack)
- Native **CQPublisher** for KubeMQ Commands and Queries (request-reply)
- Full Watermill Router and middleware compatibility
- Streaming APIs for high-throughput publishing
- Watermill Ack/Nack bridged to KubeMQ queue settlement
- Built-in DLQ support via `QueueMessagePolicy`
- OpenTelemetry trace propagation via W3C Trace Context
- Prometheus metrics via Watermill's built-in metrics component
- Passes the full Watermill `pubsub/tests.TestPubSub` compatibility suite

## Installation

```bash
go get github.com/kubemq-io/watermill-kubemq
```

**Requirements:**
- Go 1.25+
- KubeMQ broker (see [Running KubeMQ](#running-kubemq) below)

## Running KubeMQ

Start a local KubeMQ broker with Docker Compose:

```bash
docker-compose up -d
```

Or run the container directly:

```bash
docker run -d \
  -p 50000:50000 \
  -p 8080:8080 \
  -p 9090:9090 \
  europe-docker.pkg.dev/kubemq/images/kubemq-next:latest
```

The broker exposes:
- `50000` -- gRPC API (used by this plugin)
- `8080` -- REST API and health endpoint
- `9090` -- Prometheus metrics

## Pattern Selection Guide

KubeMQ supports three messaging patterns. Each Publisher/Subscriber instance serves exactly one pattern -- choose the one that matches your use case:

| Pattern | Delivery | Ack/Nack | Persistence | Best For |
|---------|----------|----------|-------------|----------|
| **Events** | At-most-once | No (fire-and-forget) | No | Real-time notifications, metrics, logs |
| **EventsStore** | At-least-once | Offset auto-advance | Yes (replay from any point) | Event sourcing, audit trails, stream replay |
| **Queues** | At-least-once | Explicit ack/nack | Yes (until acked) | Task queues, job processing, reliable delivery |

**Decision flow:**
1. Need guaranteed delivery with explicit acknowledgment? Use **Queues**.
2. Need event replay or persistence? Use **EventsStore**.
3. Need lowest latency, fire-and-forget? Use **Events**.

## Quickstart

### Events (Fire-and-Forget)

```go
package main

import (
    "context"
    "log"

    "github.com/ThreeDotsLabs/watermill"
    "github.com/ThreeDotsLabs/watermill/message"
    "github.com/ThreeDotsLabs/watermill/message/router/middleware"
    kubemq "github.com/kubemq-io/watermill-kubemq/pkg/kubemq"
)

func main() {
    logger := watermill.NewStdLogger(false, false)

    pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
        Address: "localhost:50000",
        Pattern: kubemq.PatternEvents,
        Logger:  logger,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer pub.Close()

    sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
        Address: "localhost:50000",
        Pattern: kubemq.PatternEvents,
        Logger:  logger,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer sub.Close()

    router, err := message.NewRouter(message.RouterConfig{}, logger)
    if err != nil {
        log.Fatal(err)
    }

    router.AddMiddleware(middleware.Recoverer)
    router.AddHandler("events-handler", "my-topic", sub, "output-topic", pub,
        func(msg *message.Message) ([]*message.Message, error) {
            log.Printf("Received: %s", string(msg.Payload))
            return []*message.Message{msg}, nil
        },
    )

    // Publish a message
    go func() {
        msg := message.NewMessage(watermill.NewUUID(), []byte("hello events"))
        if err := pub.Publish("my-topic", msg); err != nil {
            log.Printf("Publish error: %v", err)
        }
    }()

    _ = router.Run(context.Background())
}
```

### EventsStore (Persistent with Replay)

```go
pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
    Address: "localhost:50000",
    Pattern: kubemq.PatternEventsStore,
    Logger:  logger,
})

sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
    Address:                "localhost:50000",
    Pattern:                kubemq.PatternEventsStore,
    ConsumerGroup:          "my-group",
    EventsStoreStartOption: kubemq.StartFromFirst, // replay all events
    Logger:                 logger,
})
```

**Start options for EventsStore:**

| Option | Description |
|--------|-------------|
| `StartFromNew` | Only events published after subscribing (default) |
| `StartFromFirst` | Replay all stored events from the beginning |
| `StartFromLast` | Replay the last event, then continue with new |
| `StartFromSequence` | Replay from a specific sequence number |
| `StartFromTime` | Replay from a specific point in time |
| `StartFromTimeDelta` | Replay from now minus a duration |

### Queues (Reliable with Ack/Nack)

```go
pub, err := kubemq.NewPublisher(kubemq.PublisherConfig{
    Address: "localhost:50000",
    Pattern: kubemq.PatternQueues,
    QueueMessagePolicy: &kubemq.QueueMessagePolicy{
        MaxReceiveCount: 3,              // After 3 failed deliveries
        MaxReceiveQueue: "tasks.dlq",    // Route to DLQ channel
    },
    Logger: logger,
})

sub, err := kubemq.NewSubscriber(kubemq.SubscriberConfig{
    Address:            "localhost:50000",
    Pattern:            kubemq.PatternQueues,
    ConsumerGroup:      "workers",
    MaxItems:           10,    // Messages per poll batch
    WaitTimeoutSeconds: 1,     // Server-side poll timeout
    Logger:             logger,
})
```

Messages delivered via Queues support explicit acknowledgment:
- `msg.Ack()` -- acknowledges the message, removing it from the queue
- `msg.Nack()` -- negative-acknowledges, returning the message to the queue for redelivery

## Configuration Reference

### PublisherConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `Address` | `string` | (required) | KubeMQ server `host:port`. Not needed if `ExistingClient` is set. |
| `ClientID` | `string` | auto-generated UUID | Unique identifier for this publisher |
| `AuthToken` | `string` | `""` | JWT or token for authentication |
| `TLS` | `*TLSConfig` | `nil` | TLS configuration (cert, key, CA) |
| `Pattern` | `PatternType` | (required) | `PatternEvents`, `PatternEventsStore`, or `PatternQueues` |
| `Marshaler` | `Marshaler` | `DefaultMarshaler{}` | Custom message marshaling |
| `ExistingClient` | `*kubemq.Client` | `nil` | Reuse an existing kubemq-go client |
| `DisableStreaming` | `bool` | `false` | Use synchronous sends instead of streaming APIs |
| `QueueMessagePolicy` | `*QueueMessagePolicy` | `nil` | DLQ and expiration settings (Queues only) |
| `Logger` | `LoggerAdapter` | `NopLogger{}` | Watermill logger |

### SubscriberConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `Address` | `string` | (required) | KubeMQ server `host:port` |
| `ClientID` | `string` | auto-generated UUID | Unique identifier for this subscriber |
| `AuthToken` | `string` | `""` | JWT or token for authentication |
| `TLS` | `*TLSConfig` | `nil` | TLS configuration |
| `Pattern` | `PatternType` | (required) | `PatternEvents`, `PatternEventsStore`, or `PatternQueues` |
| `Unmarshaler` | `Unmarshaler` | `DefaultMarshaler{}` | Custom message unmarshaling |
| `ExistingClient` | `*kubemq.Client` | `nil` | Reuse an existing kubemq-go client |
| `ConsumerGroup` | `string` | `""` | Competing consumers group name |
| `MaxItems` | `int32` | `1` | Messages per poll (Queues only, 1-1000) |
| `WaitTimeoutSeconds` | `int32` | `1` | Server-side poll timeout (Queues only) |
| `EventsStoreStartOption` | `EventsStoreStartOption` | `StartFromNew` | Where to start consuming (EventsStore only) |
| `EventsStoreSequence` | `int64` | `0` | Sequence number for `StartFromSequence` |
| `EventsStoreStartTime` | `time.Time` | zero | Start time for `StartFromTime` |
| `EventsStoreTimeDelta` | `time.Duration` | `0` | Duration for `StartFromTimeDelta` |
| `Logger` | `LoggerAdapter` | `NopLogger{}` | Watermill logger |

### CQConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `Address` | `string` | (required) | KubeMQ server `host:port` |
| `ClientID` | `string` | auto-generated UUID | Unique identifier |
| `AuthToken` | `string` | `""` | Authentication token |
| `TLS` | `*TLSConfig` | `nil` | TLS configuration |
| `DefaultTimeout` | `time.Duration` | (required) | Default timeout for commands/queries |
| `ExistingClient` | `*kubemq.Client` | `nil` | Reuse an existing client |
| `Logger` | `LoggerAdapter` | `NopLogger{}` | Watermill logger |

### QueueMessagePolicy

| Field | Type | Description |
|-------|------|-------------|
| `ExpirationSeconds` | `int32` | Message TTL in seconds (0 = no expiration) |
| `DelaySeconds` | `int32` | Delay before message becomes visible |
| `MaxReceiveCount` | `int32` | Max delivery attempts before DLQ routing |
| `MaxReceiveQueue` | `string` | DLQ channel name |

## Middleware Compatibility

All standard Watermill middleware works with this plugin. Some middleware is only meaningful for specific patterns:

| Middleware | Events | EventsStore | Queues | Notes |
|-----------|--------|-------------|--------|-------|
| Retry | N/A | N/A | Yes | Nack triggers redelivery; only meaningful for Queues |
| Throttle | Yes | Yes | Yes | Rate-limits handler invocations |
| CorrelationID | Yes | Yes | Yes | Stored in Tags via Metadata |
| Timeout | Yes | Yes | Yes | Context deadline propagated |
| Poison Queue | N/A | N/A | Yes | Application-level DLQ (alternative to KubeMQ native DLQ) |
| CircuitBreaker | Yes | Yes | Yes | Handler-level |
| Deduplication | Yes | Yes | Yes | Uses UUID from Tags |
| InstantAck | Yes | Yes | N/A | For fire-and-forget patterns |
| Recoverer | Yes | Yes | Yes | Panic recovery |
| Prometheus | Yes | Yes | Yes | Built-in metrics component |
| OpenTelemetry | Yes | Yes | Yes | Community middleware |

## CQRS Example

The Watermill CQRS component works with KubeMQ by using Queues for commands (reliable delivery) and EventsStore for domain events (persistence and replay):

```go
package main

import (
    "context"
    "log"

    "github.com/ThreeDotsLabs/watermill"
    "github.com/ThreeDotsLabs/watermill/components/cqrs"
    "github.com/ThreeDotsLabs/watermill/message"
    kubemq "github.com/kubemq-io/watermill-kubemq/pkg/kubemq"
)

type CreateOrderCmd struct{ OrderID string }
type OrderCreatedEvt struct{ OrderID string }

func main() {
    logger := watermill.NewStdLogger(false, false)
    router, _ := message.NewRouter(message.RouterConfig{}, logger)

    // Commands via Queues (reliable, exactly-once processing)
    cmdPub, _ := kubemq.NewPublisher(kubemq.PublisherConfig{
        Address: "localhost:50000",
        Pattern: kubemq.PatternQueues,
    })
    cmdSub, _ := kubemq.NewSubscriber(kubemq.SubscriberConfig{
        Address: "localhost:50000",
        Pattern: kubemq.PatternQueues,
    })

    // Events via EventsStore (persistent, replayable)
    evtPub, _ := kubemq.NewPublisher(kubemq.PublisherConfig{
        Address: "localhost:50000",
        Pattern: kubemq.PatternEventsStore,
    })
    evtSub, _ := kubemq.NewSubscriber(kubemq.SubscriberConfig{
        Address:                "localhost:50000",
        Pattern:                kubemq.PatternEventsStore,
        EventsStoreStartOption: kubemq.StartFromNew,
    })

    // Topic naming convention: "commands.CreateOrderCmd", "events.OrderCreatedEvt"
    _ = cmdPub; _ = cmdSub; _ = evtPub; _ = evtSub
    _ = cqrs.NewFacade // construct with CommandHandlers, EventHandlers, router, marshaler

    _ = router.Run(context.Background())
}

func handleCreateOrder(ctx context.Context, cmd *CreateOrderCmd) error {
    log.Printf("Creating order: %s", cmd.OrderID)
    return nil // publish OrderCreatedEvt via event bus
}

func onOrderCreated(ctx context.Context, evt *OrderCreatedEvt) error {
    log.Printf("Order created: %s", evt.OrderID)
    return nil
}
```

## Request-Reply (CQ)

Two approaches for request-reply, depending on your requirements:

### Approach 1: Watermill requestreply Component (Middleware Compatible)

Uses standard Watermill Publisher/Subscriber with the `requestreply` component. All Watermill middleware works. Higher latency (two hops via queue/events).

```go
import "github.com/ThreeDotsLabs/watermill/components/requestreply"

reqPub, _ := kubemq.NewPublisher(kubemq.PublisherConfig{
    Address: "localhost:50000",
    Pattern: kubemq.PatternQueues,
})
replySub, _ := kubemq.NewSubscriber(kubemq.SubscriberConfig{
    Address: "localhost:50000",
    Pattern: kubemq.PatternQueues,
})

backend := requestreply.NewPubSubBackend(reqPub,
    requestreply.NewPubSubBackendSubscriberConfig(replySub, "reply-topic"),
)
// Use backend.SendCommandToBackend(ctx, "request-topic", msg)
```

### Approach 2: Native CQPublisher (Lower Latency)

Wraps KubeMQ's native Commands and Queries APIs directly. Single hop, lower latency, but bypasses Watermill middleware.

```go
import "time"

cq, _ := kubemq.NewCQPublisher(kubemq.CQConfig{
    Address:        "localhost:50000",
    DefaultTimeout: 5 * time.Second,
})
defer cq.Close()

// Send a query
msg := message.NewMessage(watermill.NewUUID(), []byte(`{"prompt":"classify this text"}`))
reply, err := cq.SendQuery(ctx, "ml.classify", msg, 10*time.Second)
if err != nil {
    log.Fatalf("Query failed: %v", err)
}
log.Printf("Reply: %s", string(reply.Payload))

// Send a command (fire-and-forget with execution confirmation)
cmdMsg := message.NewMessage(watermill.NewUUID(), []byte(`{"action":"deploy"}`))
resp, err := cq.SendCommand(ctx, "ops.deploy", cmdMsg, 30*time.Second)
```

## DLQ (Dead Letter Queue)

Two DLQ mechanisms can be used independently or together:

**KubeMQ Native DLQ** -- handles transport-level failures (consumer crashes, unacked messages):

```go
pub, _ := kubemq.NewPublisher(kubemq.PublisherConfig{
    Address: "localhost:50000",
    Pattern: kubemq.PatternQueues,
    QueueMessagePolicy: &kubemq.QueueMessagePolicy{
        MaxReceiveCount: 3,
        MaxReceiveQueue: "my-queue.dlq",
    },
})
```

**Watermill Poison Queue Middleware** -- handles application-level failures (handler panics, business logic errors):

```go
import "github.com/ThreeDotsLabs/watermill/message/router/middleware"

poisonQueue, _ := middleware.PoisonQueue(dlqPublisher, "poison-queue")
router.AddMiddleware(poisonQueue)
```

## Prometheus Metrics

Watermill's built-in Prometheus metrics component works out of the box:

```go
import (
    "github.com/ThreeDotsLabs/watermill/components/metrics"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

reg := prometheus.NewRegistry()
metricsBuilder := metrics.NewPrometheusMetricsBuilder(reg, "watermill", "kubemq")
metricsBuilder.AddPrometheusRouterMetrics(router)

// Exposes at :8081/metrics:
// - watermill_kubemq_publish_time_seconds (histogram)
// - watermill_kubemq_subscriber_received_message_time_seconds (histogram)
// - watermill_kubemq_handler_execution_time_seconds (histogram)
go func() { http.ListenAndServe(":8081", promhttp.HandlerFor(reg, promhttp.HandlerOpts{})) }()
```

## OpenTelemetry Trace Propagation

Trace context is automatically propagated via KubeMQ Tags using W3C Trace Context format. The kubemq-go/v2 SDK has built-in OTel integration. At the Watermill level, existing community OpenTelemetry middleware can be used for additional instrumentation.

**OTel span attributes set by the plugin:**

| Attribute | Value |
|-----------|-------|
| `messaging.system` | `"kubemq"` |
| `messaging.operation.name` | `"publish"`, `"receive"`, `"process"` |
| `messaging.destination.name` | Topic/channel name |
| `messaging.message.id` | Watermill UUID |
| `messaging.client.id` | KubeMQ ClientID |
| `messaging.consumer.group.name` | ConsumerGroup (if set) |
| `server.address` | KubeMQ host |
| `server.port` | KubeMQ port |

## KEDA Integration

KubeMQ has an existing [KEDA](https://keda.sh/) scaler for Kubernetes-native autoscaling. Use it to scale your Watermill consumers based on queue depth.

Install the KubeMQ KEDA scaler from the [kubemq-keda](https://github.com/kubemq-io/kubemq-keda) repository, then create a `ScaledObject`:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: watermill-worker
spec:
  scaleTargetRef:
    name: watermill-worker-deployment
  minReplicaCount: 1
  maxReplicaCount: 20
  triggers:
    - type: kubemq
      metadata:
        address: "kubemq.kubemq.svc.cluster.local:50000"
        channel: "ml.requests"
        queueLength: "10"
```

This scales your Watermill Queue subscriber pods based on queue depth -- when the queue grows beyond the threshold, KEDA adds more pods running your Watermill consumer.

## Health Checks

Both Publisher and Subscriber expose a `HealthCheck` method that verifies broker connectivity:

```go
if err := pub.HealthCheck(ctx); err != nil {
    log.Printf("Publisher unhealthy: %v", err)
}

if err := sub.HealthCheck(ctx); err != nil {
    log.Printf("Subscriber unhealthy: %v", err)
}
```

## Examples

See the [`_examples/`](_examples/) directory for complete, runnable examples:

| Example | Description |
|---------|-------------|
| [`basic-pubsub`](_examples/basic-pubsub/) | Events pattern with Router and middleware |
| [`queue-workers`](_examples/queue-workers/) | Competing consumers with DLQ and Poison Queue |
| [`event-sourcing`](_examples/event-sourcing/) | EventsStore with replay from specific positions |
| [`request-reply`](_examples/request-reply/) | Both CQ approaches (requestreply component + native CQPublisher) |
| [`ml-inference`](_examples/ml-inference/) | Full ML pipeline with KEDA and Prometheus |
| [`cqrs`](_examples/cqrs/) | CQRS with CommandBus (Queues) and EventBus (EventsStore) |

## Development

```bash
# Run unit tests
make test

# Run unit tests with race detector
make test-race

# Run integration tests (requires running KubeMQ broker)
docker-compose up -d
make test-integration

# Run Watermill compatibility suite
make test-compatibility

# Lint
make lint

# Generate coverage report
make coverage
```

## License

Apache 2.0 -- see [LICENSE](LICENSE) for details.
