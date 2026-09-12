// Copyright 2026 Envoyproxy Authors
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package sotw_test

import (
	"context"
	"testing"
	"time"

	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"

	envoycorev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoyendpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	rsrc "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/config"
	sotw "github.com/envoyproxy/go-control-plane/pkg/server/sotw/v3"
	server "github.com/envoyproxy/go-control-plane/pkg/server/v3"
)

func TestStaleNonceDropsSubscriptionChangeAndWatch(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		name := "default"
		if ordered {
			name = "ordered"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			c := cache.NewSnapshotCache(true, kgwHash{}, nil)
			if err := c.SetSnapshot(ctx, kgwNode, kgwEDS(t, "v1", cla("a", 1))); err != nil {
				t.Fatal(err)
			}

			observed := make(chan *discovery.DiscoveryRequest, 8)
			callbacks := server.CallbackFuncs{StreamRequestFunc: func(_ int64, req *discovery.DiscoveryRequest) error {
				observed <- req
				return nil
			}}
			srv := server.NewServer(ctx, c, callbacks)
			if ordered {
				srv = server.NewServer(ctx, c, callbacks, sotw.WithOrderedADS())
			}
			s := &scriptedStream{ctx: ctx, recv: make(chan *discovery.DiscoveryRequest), sent: make(chan *discovery.DiscoveryResponse)}
			done := make(chan error, 1)
			go func() { done <- srv.StreamAggregatedResources(s) }()
			t.Cleanup(func() {
				cancel()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("server did not shut down")
				}
			})

			first := sendAndReceive(t, s, observed, &discovery.DiscoveryRequest{
				Node:          &envoycorev3.Node{Id: kgwNode},
				TypeUrl:       rsrc.EndpointType,
				ResourceNames: []string{"a"},
			})
			sendAndObserve(t, s, observed, &discovery.DiscoveryRequest{
				TypeUrl:       rsrc.EndpointType,
				ResourceNames: []string{"a"},
				VersionInfo:   first.VersionInfo,
				ResponseNonce: first.Nonce,
			})
			waitForWatches(t, c, 1)

			if err := c.SetSnapshot(ctx, kgwNode, kgwEDS(t, "v2", cla("a", 2))); err != nil {
				t.Fatal(err)
			}
			second := receive(t, s)
			if second.Nonce == first.Nonce {
				t.Fatalf("expected a new nonce, got %q", second.Nonce)
			}

			// This is an ACK of the first response and a full SotW subscription
			// change to {a,b}. Because the second response crossed it in flight,
			// the server regards the otherwise valid request as stale.
			sendAndObserve(t, s, observed, &discovery.DiscoveryRequest{
				TypeUrl:       rsrc.EndpointType,
				ResourceNames: []string{"a", "b"},
				VersionInfo:   first.VersionInfo,
				ResponseNonce: first.Nonce,
			})
			// Callbacks precede nonce validation. Observing a second request
			// on this serial loop establishes that processing the first stale
			// request finished; its own callback alone is not that barrier.
			sendAndObserve(t, s, observed, &discovery.DiscoveryRequest{
				TypeUrl:       rsrc.EndpointType,
				ResourceNames: []string{"a", "b"},
				VersionInfo:   first.VersionInfo,
				ResponseNonce: first.Nonce,
			})
			waitForWatches(t, c, 0)

			if err := c.SetSnapshot(ctx, kgwNode, kgwEDS(t, "v3", cla("a", 3), cla("b", 1))); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-s.sent:
				t.Fatalf("stale request unexpectedly installed a watch: %v", got)
			default:
			}

			// A request carrying the current nonce is processed, installs {a,b},
			// and immediately receives the already-installed v3 snapshot.
			third := sendAndReceive(t, s, observed, &discovery.DiscoveryRequest{
				TypeUrl:       rsrc.EndpointType,
				ResourceNames: []string{"a", "b"},
				VersionInfo:   second.VersionInfo,
				ResponseNonce: second.Nonce,
			})
			if third.VersionInfo != "v3" || len(third.Resources) != 2 {
				t.Fatalf("current-nonce request did not recover with {a,b}: %v", third)
			}
		})
	}
}

func sendAndObserve(t *testing.T, s *scriptedStream, observed <-chan *discovery.DiscoveryRequest, req *discovery.DiscoveryRequest) {
	t.Helper()
	select {
	case s.recv <- req:
	case <-time.After(time.Second):
		t.Fatal("request not consumed")
	}
	select {
	case got := <-observed:
		if got != req {
			t.Fatalf("callback observed a different request: got %p, want %p", got, req)
		}
	case <-time.After(time.Second):
		t.Fatal("request did not reach server callback")
	}
}

func sendAndReceive(t *testing.T, s *scriptedStream, observed <-chan *discovery.DiscoveryRequest, req *discovery.DiscoveryRequest) *discovery.DiscoveryResponse {
	t.Helper()
	sendAndObserve(t, s, observed, req)
	return receive(t, s)
}

func receive(t *testing.T, s *scriptedStream) *discovery.DiscoveryResponse {
	t.Helper()
	select {
	case response := <-s.sent:
		return response
	case <-time.After(time.Second):
		t.Fatal("no response")
	}
	return nil
}

func waitForWatches(t *testing.T, c cache.SnapshotCache, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for c.GetStatusInfo(kgwNode).GetNumWatches() != want {
		if time.Now().After(deadline) {
			t.Fatalf("watch count did not become %d; got %d", want, c.GetStatusInfo(kgwNode).GetNumWatches())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestStaleNonceSubscriptionUpdates(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		for _, damping := range []bool{false, true} {
			name := "default-ads"
			if ordered {
				name = "ordered-ads"
			}
			if damping {
				name += "/damping"
			}
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				c := observedWatchCache{SnapshotCache: cache.NewSnapshotCacheWithOptions(kgwHash{}, nil, cache.WithADS(), cache.WithSubscriptionFilteredResponses()), requests: make(chan *discovery.DiscoveryRequest, 1)}
				publish := func(version string, b bool) {
					t.Helper()
					resources := []*envoyendpointv3.ClusterLoadAssignment{cla("a", 1)}
					if b {
						resources = append(resources, cla("b", 1))
					}
					if err := c.SetSnapshot(ctx, kgwNode, kgwEDS(t, version, resources...)); err != nil {
						t.Fatal(err)
					}
				}
				publish("v1", false)
				observed := make(chan *discovery.DiscoveryRequest, 1)
				callbacks := server.CallbackFuncs{StreamRequestFunc: func(_ int64, req *discovery.DiscoveryRequest) error { observed <- req; return nil }}
				opts := []config.XDSOption{server.WithStaleNonceSubscriptionUpdates()}
				if ordered {
					opts = append(opts, sotw.WithOrderedADS())
				}
				if damping {
					opts = append(opts, server.WithNackDamping())
				}
				srv := server.NewServer(ctx, c, callbacks, opts...)
				s := &scriptedStream{ctx: ctx, recv: make(chan *discovery.DiscoveryRequest), sent: make(chan *discovery.DiscoveryResponse)}
				done := make(chan error, 1)
				go func() { done <- srv.StreamAggregatedResources(s) }()
				t.Cleanup(func() {
					cancel()
					select {
					case <-done:
					case <-time.After(time.Second):
						t.Error("server did not shut down")
					}
				})
				submit := func(req *discovery.DiscoveryRequest) *discovery.DiscoveryRequest {
					t.Helper()
					sendAndObserve(t, s, observed, req)
					select {
					case got := <-c.requests:
						return got
					case <-time.After(time.Second):
						t.Fatal("subscription request did not reach cache")
					}
					return nil
				}
				submit(&discovery.DiscoveryRequest{Node: &envoycorev3.Node{Id: kgwNode}, TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}})
				first := receive(t, s)
				submit(&discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}, VersionInfo: first.VersionInfo, ResponseNonce: first.Nonce})
				waitForWatches(t, c, 1)
				publish("v2", false)
				second := receive(t, s)
				// Both a stale ACK and a stale NACK carry names, but neither accepts or
				// rejects the current response. Repetition must retain the current nonce.
				for _, rejected := range []bool{false, true} {
					req := &discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a", "b"}, VersionInfo: first.VersionInfo, ResponseNonce: first.Nonce}
					if rejected {
						req.ErrorDetail = &rpcstatus.Status{Code: 3, Message: "old rejection"}
					}
					cached := submit(req)
					if cached == req || cached.VersionInfo != second.VersionInfo || cached.ErrorDetail != nil {
						t.Fatalf("stale request used as acknowledgment or rejection: %v", cached)
					}
					if req.VersionInfo != first.VersionInfo || (req.ErrorDetail != nil) != rejected {
						t.Fatal("original callback request was changed")
					}
					waitForWatches(t, c, 1)
				}
				select {
				case got := <-s.sent:
					t.Fatalf("stale version triggered an unwanted resend: %v", got)
				default:
				}
				publish("v3", true)
				third := receive(t, s)
				if third.VersionInfo != "v3" || len(third.Resources) != 2 {
					t.Fatalf("stale subscription did not receive a and b: %v", third)
				}
				ack := &discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a", "b"}, VersionInfo: third.VersionInfo, ResponseNonce: third.Nonce}
				if submit(ack) != ack {
					t.Fatal("current ACK was replaced")
				}
				waitForWatches(t, c, 1)
				// A stale shrink also takes effect without replaying the stale version.
				submit(&discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}, VersionInfo: first.VersionInfo, ResponseNonce: first.Nonce})
				waitForWatches(t, c, 1)
				select {
				case got := <-s.sent:
					t.Fatalf("a stale shrink must not be answered: %v", got)
				case <-time.After(50 * time.Millisecond):
				}
				publish("v4", true)
				fourth := receive(t, s)
				if fourth.VersionInfo != "v4" || len(fourth.Resources) != 1 {
					t.Fatalf("stale shrink was not applied: %v", fourth)
				}
				endpoint := &envoyendpointv3.ClusterLoadAssignment{}
				if err := fourth.Resources[0].UnmarshalTo(endpoint); err != nil {
					t.Fatal(err)
				}
				if endpoint.ClusterName != "a" {
					t.Fatalf("unsubscribed resource sent: %q", endpoint.ClusterName)
				}
			})
		}
	}
}

// A stale request that adds a name the snapshot already holds is answered at
// once: the cloned request carries the last sent version and names a resource
// not yet returned, so the cache responds with that resource at the current
// version. This is the one case in which the option sends a response to a
// stale-nonce request, and it is what the client asked for. The response is
// acknowledged with its own nonce and the stream continues normally.
func TestStaleNonceSubscriptionExpansionIsAnsweredWhenPresent(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		name := map[bool]string{false: "default-ads", true: "ordered-ads"}[ordered]
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			c := observedWatchCache{SnapshotCache: cache.NewSnapshotCacheWithOptions(kgwHash{}, nil, cache.WithADS(), cache.WithSubscriptionFilteredResponses()), requests: make(chan *discovery.DiscoveryRequest, 1)}
			publish := func(version string) {
				t.Helper()
				if err := c.SetSnapshot(ctx, kgwNode, kgwEDS(t, version, cla("a", 1), cla("b", 1))); err != nil {
					t.Fatal(err)
				}
			}
			publish("v1")
			observed := make(chan *discovery.DiscoveryRequest, 1)
			callbacks := server.CallbackFuncs{StreamRequestFunc: func(_ int64, req *discovery.DiscoveryRequest) error { observed <- req; return nil }}
			opts := []config.XDSOption{server.WithStaleNonceSubscriptionUpdates()}
			if ordered {
				opts = append(opts, sotw.WithOrderedADS())
			}
			srv := server.NewServer(ctx, c, callbacks, opts...)
			s := &scriptedStream{ctx: ctx, recv: make(chan *discovery.DiscoveryRequest), sent: make(chan *discovery.DiscoveryResponse)}
			done := make(chan error, 1)
			go func() { done <- srv.StreamAggregatedResources(s) }()
			t.Cleanup(func() {
				cancel()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("server did not shut down")
				}
			})
			submit := func(req *discovery.DiscoveryRequest) {
				t.Helper()
				sendAndObserve(t, s, observed, req)
				select {
				case <-c.requests:
				case <-time.After(time.Second):
					t.Fatal("subscription request did not reach cache")
				}
			}
			submit(&discovery.DiscoveryRequest{Node: &envoycorev3.Node{Id: kgwNode}, TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}})
			first := receive(t, s)
			submit(&discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}, VersionInfo: first.VersionInfo, ResponseNonce: first.Nonce})
			waitForWatches(t, c, 1)
			publish("v2")
			second := receive(t, s)

			// Stale expansion: the old nonce, and a name the snapshot holds.
			submit(&discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a", "b"}, VersionInfo: first.VersionInfo, ResponseNonce: first.Nonce})
			answered := receive(t, s)
			if answered.VersionInfo != second.VersionInfo || len(answered.Resources) != 1 {
				t.Fatalf("stale expansion must be answered with the new resource at the current version, got %v", answered)
			}
			endpoint := &envoyendpointv3.ClusterLoadAssignment{}
			if err := answered.Resources[0].UnmarshalTo(endpoint); err != nil {
				t.Fatal(err)
			}
			if endpoint.ClusterName != "b" {
				t.Fatalf("expected the newly subscribed resource, got %q", endpoint.ClusterName)
			}
			submit(&discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a", "b"}, VersionInfo: answered.VersionInfo, ResponseNonce: answered.Nonce})
			waitForWatches(t, c, 1)
			publish("v3")
			third := receive(t, s)
			if third.VersionInfo != "v3" || len(third.Resources) != 2 {
				t.Fatalf("stream did not continue normally after the stale expansion: %v", third)
			}
		})
	}
}
