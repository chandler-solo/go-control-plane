# xDS Server Implementation

go-control-plane ships with a full [streaming implementation](https://github.com/envoyproxy/go-control-plane/blob/main/pkg/server/v3/server.go#L175) of the xDS protocol. Current support for the servers lists as follows:
- REST HTTP/1.1 *(This will soon be deprecated)*
- gRPC Bi-Di
	- State of the World
	- Incremental

## Getting Started

For a fully functional gRPC server, check out the provided example for what that looks like:
- https://github.com/envoyproxy/go-control-plane/blob/main/internal/example/server.go

### Callbacks

All go-control-plane xDS server implementations require `Callback` methods. Callbacks are executed at certain steps of the management server lifecycle. The interface to be implemented can be found [here](https://godoc.org/github.com/envoyproxy/go-control-plane/pkg/server/v2#Callbacks).

An example implemention of the Callback interface can be found below:
```go
import (
	"context"
	"log"
	"sync"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

type Callbacks struct {
	Signal         chan struct{}
	Debug          bool
	Fetches        int
	Requests       int
	DeltaRequests  int
	DeltaResponses int
	mu             sync.Mutex
}

func (cb *Callbacks) Report() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	log.Printf("server callbacks fetches=%d requests=%d\n", cb.Fetches, cb.Requests)
}
func (cb *Callbacks) OnStreamOpen(_ context.Context, id int64, typ string) error {
	if cb.Debug {
		log.Printf("stream %d open for %s\n", id, typ)
	}
	return nil
}
func (cb *Callbacks) OnStreamClosed(id int64, node *core.Node) {
	if cb.Debug {
		log.Printf("stream %d of node %s closed\n", id, node.Id)
	}
}
func (cb *Callbacks) OnDeltaStreamOpen(_ context.Context, id int64, typ string) error {
	if cb.Debug {
		log.Printf("delta stream %d open for %s\n", id, typ)
	}
	return nil
}
func (cb *Callbacks) OnDeltaStreamClosed(id int64, node *core.Node) {
	if cb.Debug {
		log.Printf("delta stream %d of node %s closed\n", id, node.Id)
	}
}
func (cb *Callbacks) OnStreamRequest(int64, *discovery.DiscoveryRequest) error {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.Requests++
	if cb.Signal != nil {
		close(cb.Signal)
		cb.Signal = nil
	}
	return nil
}
func (cb *Callbacks) OnStreamResponse(context.Context, int64, *discovery.DiscoveryRequest, *discovery.DiscoveryResponse) {
}
func (cb *Callbacks) OnStreamDeltaResponse(id int64, req *discovery.DeltaDiscoveryRequest, res *discovery.DeltaDiscoveryResponse) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.DeltaResponses++
}
func (cb *Callbacks) OnStreamDeltaRequest(id int64, req *discovery.DeltaDiscoveryRequest) error {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.DeltaRequests++
	if cb.Signal != nil {
		close(cb.Signal)
		cb.Signal = nil
	}

	return nil
}
func (cb *Callbacks) OnFetchRequest(_ context.Context, req *discovery.DiscoveryRequest) error {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.Fetches++
	if cb.Signal != nil {
		close(cb.Signal)
		cb.Signal = nil
	}
	return nil
}
func (cb *Callbacks) OnFetchResponse(*discovery.DiscoveryRequest, *discovery.DiscoveryResponse) {}
```

## Info

The internal go-control-plane gRPC server implementations take care of managing watches with the [Config Watcher](https://github.com/envoyproxy/go-control-plane/blob/main/pkg/cache/v3/cache.go#L45) when new xDS clients register themselves.

> *NOTE*: The server supports REST/JSON as well as gRPC bi-di streaming

## Damping repeated SotW NACKs

By default, a NACK can immediately resend the same snapshot version because
the request carries the client's last accepted version. To wait for a changed
version after rejection, construct the server with `server.WithNackDamping()`:

```go
srv := server.NewServer(ctx, snapshotCache, callbacks, server.WithNackDamping())
```

The option applies to dedicated SotW streams and both ADS modes. For a NACK
whose nonce matches the last response of that type, the server gives the cache
a cloned request carrying the rejected version. The original request and its
error detail remain available to `OnStreamRequest`. The cache parks unchanged
subscriptions until the snapshot version changes; a corrected version is sent
normally. Subscription changes can still request newly subscribed resources.

Damping is disabled by default and does not affect delta streams. Control
planes must change the version when correcting configuration. Changing content
without a new version cannot reliably trigger a response, even without damping.

Two details of the option are visible to clients. First, Envoy keeps the error
detail on every request for a type until it accepts a new version, so a
subscription change made while rejected arrives as a NACK; damping still lets
the cache answer it, because the request names a resource not yet returned,
and that response carries the rejected version. The client rejects it once
more and the stream parks again, so re-sends of rejected content are bounded
by the client's own subscription changes. Second, with damping on the server
carries the last nonce and version onto the watch that replaces a completed
one, so a later request with an older nonce is treated as stale even though no
response has been sent since; without damping such a request is accepted.

## Subscription changes on stale SotW requests

`server.WithStaleNonceSubscriptionUpdates()` is an experimental opt-in policy
for SotW streams, including both ADS modes. The default policy ignores requests
with stale nonces. With the option enabled, their resource-name subscriptions
are applied and a replacement cache watch is created.

A stale request does not acknowledge or reject the latest response. The cache
receives a cloned request with the last sent version and no error detail, while
`OnStreamRequest` retains the client's original request. Newly subscribed
resources and later snapshot versions can be delivered; stale accepted versions
do not themselves trigger replay. The option also preserves subscription
shrinks and can be combined with `WithNackDamping()`. Delta streams are unchanged.

One consequence should be stated plainly, because it departs from the text
quoted in that discussion, which says a server should not send a response for a
request with a stale nonce. When a stale request adds a name the snapshot
already holds, the cloned request carries the last sent version and a name not
yet returned, so the cache answers it at once with that resource at the current
version. That is a response to a stale-nonce request. It is deliberate: the
client asked for the resource and Envoy accepts a response regardless of the
nonce it last saw, acknowledging it with the new nonce. A stale request that
only removes names produces no response; the shrink is applied to the
subscription and shows in the next published version.

This policy is disabled by default pending the protocol discussion in
[Envoy #10363](https://github.com/envoyproxy/envoy/issues/10363).
