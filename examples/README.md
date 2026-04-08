# KubeMQ Watermill Integration Examples

Comprehensive examples demonstrating all features of the KubeMQ Watermill integration.

## Prerequisites

- **KubeMQ server** running on `localhost:50000`
- **Go 1.25+** installed

## How to Run

From the project root:

```bash
go run ./_examples/<category>/<example>/main.go
```

For example:

```bash
go run ./_examples/events/basic-pubsub/main.go
```

## Core Patterns

| Example | Description | Pattern |
|---------|-------------|---------|
| [events/basic-pubsub](events/basic-pubsub/main.go) | Fire-and-forget event publish and subscribe | Events |
| [events/stream-publish](events/stream-publish/main.go) | Streaming vs non-streaming publish comparison | Events |
| [events/consumer-group](events/consumer-group/main.go) | ConsumerGroup for load-balanced event delivery | Events |
| [events/multiple-subscribers](events/multiple-subscribers/main.go) | Fan-out to multiple subscribers (no consumer group) | Events |
| [events/router-handler](events/router-handler/main.go) | Watermill Router with event handler | Events |
| [events-store/basic-pubsub](events-store/basic-pubsub/main.go) | Persistent event publish and subscribe | EventsStore |
| [events-store/start-from-new](events-store/start-from-new/main.go) | StartFromNew -- only new events | EventsStore |
| [events-store/start-from-first](events-store/start-from-first/main.go) | StartFromFirst -- replay all stored events | EventsStore |
| [events-store/start-from-last](events-store/start-from-last/main.go) | StartFromLast -- latest stored event only | EventsStore |
| [events-store/start-from-sequence](events-store/start-from-sequence/main.go) | StartFromSequence -- from sequence number N | EventsStore |
| [events-store/start-from-time](events-store/start-from-time/main.go) | StartFromTime -- from absolute timestamp | EventsStore |
| [events-store/start-from-time-delta](events-store/start-from-time-delta/main.go) | StartFromTimeDelta -- from relative time offset | EventsStore |
| [events-store/consumer-group](events-store/consumer-group/main.go) | ConsumerGroup with persistent events | EventsStore |
| [queues/send-receive](queues/send-receive/main.go) | Basic queue send and receive with ack | Queues |
| [queues/ack-nack](queues/ack-nack/main.go) | Explicit ack and nack handling | Queues |
| [queues/dead-letter-queue](queues/dead-letter-queue/main.go) | Dead letter queue with MaxReceiveCount | Queues |
| [queues/delayed-messages](queues/delayed-messages/main.go) | Delayed message delivery with DelaySeconds | Queues |
| [queues/expiration](queues/expiration/main.go) | Message expiration with ExpirationSeconds | Queues |
| [queues/competing-consumers](queues/competing-consumers/main.go) | ConsumerGroup for work distribution | Queues |
| [queues/batch-send](queues/batch-send/main.go) | Publish multiple messages in one call | Queues |
| [queues/router-handler](queues/router-handler/main.go) | Watermill Router with queue handler | Queues |

## Watermill Features

| Example | Description |
|---------|-------------|
| router/* | Router-based message handling with AddHandler and AddNoPublisherHandler |
| middleware/* | Built-in and custom middleware (Recoverer, Retry, PoisonQueue, Throttle, etc.) |
| cqrs/* | CQRS facade wired with KubeMQ pub/sub |

## Infrastructure

| Example | Description |
|---------|-------------|
| connection/* | Connection management, client reuse, auth tokens, health checks |
| tls/* | TLS configuration: cert file, cert data, insecure skip verify |
| error-handling/* | Error handling patterns for publish, subscribe, and graceful shutdown |
| observability/* | OTel trace propagation, logging, health check endpoints |

## Advanced Patterns

| Example | Description |
|---------|-------------|
| advanced/fan-out-events | Fan-out with message counting across multiple subscribers |
| advanced/competing-consumers | Router-based competing consumers with queue workers |
| advanced/request-reply-watermill | Watermill requestreply component with KubeMQ transport |
| advanced/request-reply-cq | Native CQPublisher for command/query request-reply |
| advanced/metadata-roundtrip | Rich metadata round-trip through DefaultMarshaler |
| advanced/custom-marshaler | Custom MarshalerUnmarshaler with gzip compression |
| advanced/non-streaming-publish | DisableStreaming mode for all three patterns |

## KubeMQ Patterns Overview

- **Events** (`PatternEvents`): Fire-and-forget pub/sub. No persistence, no ack required. Subscriber must be active before publish.
- **EventsStore** (`PatternEventsStore`): Persistent pub/sub with replay. Supports 6 start options (New, First, Last, Sequence, Time, TimeDelta).
- **Queues** (`PatternQueues`): Reliable message queues with explicit ack/nack. Supports DLQ, delayed delivery, expiration, competing consumers.
- **Commands** (via `CQPublisher`): Request-reply with execution confirmation. Timeout-based.
- **Queries** (via `CQPublisher`): Request-reply with data response. Supports caching.

## Watermill Concepts

- **Publisher/Subscriber**: Direct API for publish and subscribe operations.
- **Router**: Message routing with handlers, middleware, and lifecycle management.
- **Middleware**: Cross-cutting concerns (retry, recovery, throttle, correlation, poison queue).
- **CQRS**: Command Query Responsibility Segregation facade.

## Channel Naming

- Events examples use `watermill-events.` prefix (e.g., `watermill-events.basic-pubsub`)
- Events-store examples use abbreviated `es` prefix (e.g., `watermill-es.basic-pubsub`) for channel names
- Queue examples use `watermill-queues.` prefix (e.g., `watermill-queues.send-receive`)
- Command/Query examples use `watermill-cq.` prefix (e.g., `watermill-cq.send-command`)
