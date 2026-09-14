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
	"google.golang.org/grpc"

	envoyclusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	envoycorev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	rsrc "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/config"
	sotw "github.com/envoyproxy/go-control-plane/pkg/server/sotw/v3"
	server "github.com/envoyproxy/go-control-plane/pkg/server/v3"
)

type scriptedStream struct {
	grpc.ServerStream
	ctx  context.Context
	recv chan *discovery.DiscoveryRequest
	sent chan *discovery.DiscoveryResponse
}

func (s *scriptedStream) Context() context.Context { return s.ctx }
func (s *scriptedStream) Recv() (*discovery.DiscoveryRequest, error) {
	select {
	case r := <-s.recv:
		return r, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *scriptedStream) Send(r *discovery.DiscoveryResponse) error {
	select {
	case s.sent <- r:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *scriptedStream) exchange(t *testing.T, r *discovery.DiscoveryRequest) *discovery.DiscoveryResponse {
	t.Helper()
	select {
	case s.recv <- r:
	case <-time.After(time.Second):
		t.Fatal("request not consumed")
	}
	select {
	case response := <-s.sent:
		return response
	case <-time.After(time.Second):
		t.Fatal("no response")
	}
	return nil
}

func TestRepeatedNackResendsSameVersionAndCorrectionRecovers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := cache.NewSnapshotCache(true, kgwHash{}, nil)
	publish := func(version string) {
		t.Helper()
		snap, err := cache.NewSnapshot(version, map[rsrc.Type][]types.Resource{rsrc.ClusterType: {&envoyclusterv3.Cluster{Name: "a"}}})
		if err != nil {
			t.Fatal(err)
		}
		if err = c.SetSnapshot(ctx, kgwNode, snap); err != nil {
			t.Fatal(err)
		}
	}
	publish("rejected")
	s := &scriptedStream{ctx: ctx, recv: make(chan *discovery.DiscoveryRequest), sent: make(chan *discovery.DiscoveryResponse)}
	done := make(chan error, 1)
	go func() { done <- server.NewServer(ctx, c, server.CallbackFuncs{}).StreamAggregatedResources(s) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("stream did not shut down")
		}
	})
	resp := s.exchange(t, &discovery.DiscoveryRequest{Node: &envoycorev3.Node{Id: kgwNode}, TypeUrl: rsrc.ClusterType})
	for i := range 32 {
		previous := resp.GetNonce()
		resp = s.exchange(t, &discovery.DiscoveryRequest{TypeUrl: rsrc.ClusterType, ResponseNonce: previous, ErrorDetail: &rpcstatus.Status{Code: 3, Message: "scripted rejection"}})
		if resp.GetVersionInfo() != "rejected" || resp.GetNonce() == previous {
			t.Fatalf("unexpected retry at %d: %v", i, resp)
		}
	}
	publish("corrected")
	resp = s.exchange(t, &discovery.DiscoveryRequest{TypeUrl: rsrc.ClusterType, ResponseNonce: resp.GetNonce(), ErrorDetail: &rpcstatus.Status{Code: 3, Message: "last rejection"}})
	if resp.GetVersionInfo() != "corrected" {
		t.Fatalf("new snapshot not delivered: %v", resp)
	}
}

// observedWatchCache reports cache requests after watch registration has completed.
// This provides a processing barrier after the server's request callbacks.
type observedWatchCache struct {
	cache.SnapshotCache
	requests chan *discovery.DiscoveryRequest
}

func (c observedWatchCache) CreateWatch(req *cache.Request, sub cache.Subscription, responses chan cache.Response) (func(), error) {
	cancel, err := c.SnapshotCache.CreateWatch(req, sub, responses)
	c.requests <- req
	return cancel, err
}

func TestNackDampingWaitsForCorrectedVersion(t *testing.T) {
	for _, mode := range []string{"ads", "ordered-ads", "dedicated"} {
		for _, named := range []bool{false, true} {
			name := "wildcard"
			if named {
				name = "named"
			}
			t.Run(mode+"/"+name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				c := observedWatchCache{SnapshotCache: cache.NewSnapshotCache(true, kgwHash{}, nil), requests: make(chan *discovery.DiscoveryRequest, 1)}
				typ := rsrc.ClusterType
				var names []string
				if named {
					typ = rsrc.EndpointType
					names = []string{"a"}
				}
				publish := func(version string) {
					t.Helper()
					resources := []types.Resource{&envoyclusterv3.Cluster{Name: "a"}}
					if named {
						resources = []types.Resource{cla("a", 1)}
					}
					snap, err := cache.NewSnapshot(version, map[rsrc.Type][]types.Resource{typ: resources})
					if err != nil {
						t.Fatal(err)
					}
					if err := c.SetSnapshot(ctx, kgwNode, snap); err != nil {
						t.Fatal(err)
					}
				}
				publish("accepted")
				observed := make(chan *discovery.DiscoveryRequest, 1)
				callbacks := server.CallbackFuncs{StreamRequestFunc: func(_ int64, req *discovery.DiscoveryRequest) error { observed <- req; return nil }}
				opts := []config.XDSOption{server.WithNackDamping()}
				if mode == "ordered-ads" {
					opts = append(opts, sotw.WithOrderedADS())
				}
				srv := server.NewServer(ctx, c, callbacks, opts...)
				s := &scriptedStream{ctx: ctx, recv: make(chan *discovery.DiscoveryRequest), sent: make(chan *discovery.DiscoveryResponse)}
				done := make(chan error, 1)
				go func() {
					if mode == "dedicated" {
						if named {
							done <- srv.StreamEndpoints(s)
						} else {
							done <- srv.StreamClusters(s)
						}
					} else {
						done <- srv.StreamAggregatedResources(s)
					}
				}()
				t.Cleanup(func() {
					cancel()
					select {
					case <-done:
					case <-time.After(time.Second):
						t.Error("server did not shut down")
					}
				})
				cacheRequest := func() *discovery.DiscoveryRequest {
					t.Helper()
					select {
					case req := <-c.requests:
						return req
					case <-time.After(time.Second):
						t.Fatal("cache request not registered")
					}
					return nil
				}
				first := sendAndReceive(t, s, observed, &discovery.DiscoveryRequest{Node: &envoycorev3.Node{Id: kgwNode}, TypeUrl: typ, ResourceNames: names})
				cacheRequest()
				sendAndObserve(t, s, observed, &discovery.DiscoveryRequest{TypeUrl: typ, ResourceNames: names, VersionInfo: first.VersionInfo, ResponseNonce: first.Nonce})
				cacheRequest()
				waitForWatches(t, c, 1)
				publish("rejected")
				rejected := receive(t, s)
				for range 32 {
					req := &discovery.DiscoveryRequest{TypeUrl: typ, ResourceNames: names, VersionInfo: "accepted", ResponseNonce: rejected.Nonce, ErrorDetail: &rpcstatus.Status{Code: 3, Message: "scripted rejection"}}
					sendAndObserve(t, s, observed, req)
					cached := cacheRequest()
					if cached == req || cached.VersionInfo != "rejected" || cached.ResponseNonce != rejected.Nonce || cached.ErrorDetail == nil {
						t.Fatalf("NACK not cloned with the rejected version: %v", cached)
					}
					if req.VersionInfo != "accepted" {
						t.Fatalf("callback request mutated: %v", req)
					}
					waitForWatches(t, c, 1)
				}
				select {
				case got := <-s.sent:
					t.Fatalf("unchanged rejected version resent: %v", got)
				case <-time.After(50 * time.Millisecond):
				}
				publish("corrected")
				corrected := receive(t, s)
				if corrected.VersionInfo != "corrected" || corrected.Nonce == rejected.Nonce {
					t.Fatalf("unexpected correction: %v", corrected)
				}
				ack := &discovery.DiscoveryRequest{TypeUrl: typ, ResourceNames: names, VersionInfo: corrected.VersionInfo, ResponseNonce: corrected.Nonce}
				sendAndObserve(t, s, observed, ack)
				if got := cacheRequest(); got != ack {
					t.Fatal("ordinary ACK was cloned or replaced")
				}
				waitForWatches(t, c, 1)
				select {
				case got := <-s.sent:
					t.Fatalf("duplicate corrected response: %v", got)
				case <-time.After(50 * time.Millisecond):
				}
			})
		}
	}
}

// Envoy keeps error_detail on every request for a type until it accepts a new
// version, so a subscription change made while rejected arrives looking like a
// NACK. Damping rewrites its version to the rejected one; the cache still
// answers because the request names a resource it has not returned, and that
// response carries the rejected version. The client rejects it once more and
// the stream parks again. This is the one path on which damping re-sends
// content at a rejected version, and it is bounded by the client's own
// subscription changes.
func TestNackDampingAnswersSubscriptionChangeWhileRejectedOnce(t *testing.T) {
	for _, mode := range []string{"ads", "ordered-ads"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			// The snapshot holds an assignment the client has not named yet. The
			// legacy ADS policy would decline every request for {a} against it, so
			// this scenario uses subscription-filtered responses; the damping
			// behavior under test is the same on either policy once the first
			// response exists.
			c := observedWatchCache{SnapshotCache: cache.NewSnapshotCacheWithOptions(kgwHash{}, nil, cache.WithADS(), cache.WithSubscriptionFilteredResponses()), requests: make(chan *discovery.DiscoveryRequest, 1)}
			publish := func(version string) {
				t.Helper()
				snap, err := cache.NewSnapshot(version, map[rsrc.Type][]types.Resource{rsrc.EndpointType: {cla("a", 1), cla("b", 1)}})
				if err != nil {
					t.Fatal(err)
				}
				if err := c.SetSnapshot(ctx, kgwNode, snap); err != nil {
					t.Fatal(err)
				}
			}
			publish("accepted")
			observed := make(chan *discovery.DiscoveryRequest, 1)
			callbacks := server.CallbackFuncs{StreamRequestFunc: func(_ int64, req *discovery.DiscoveryRequest) error { observed <- req; return nil }}
			opts := []config.XDSOption{server.WithNackDamping()}
			if mode == "ordered-ads" {
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
			cacheRequest := func() *discovery.DiscoveryRequest {
				t.Helper()
				select {
				case req := <-c.requests:
					return req
				case <-time.After(time.Second):
					t.Fatal("cache request not registered")
				}
				return nil
			}
			nack := func(names []string, nonce string) *discovery.DiscoveryRequest {
				return &discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: names, VersionInfo: "accepted", ResponseNonce: nonce, ErrorDetail: &rpcstatus.Status{Code: 3, Message: "scripted rejection"}}
			}

			first := sendAndReceive(t, s, observed, &discovery.DiscoveryRequest{Node: &envoycorev3.Node{Id: kgwNode}, TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}})
			cacheRequest()
			sendAndObserve(t, s, observed, &discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}, VersionInfo: first.VersionInfo, ResponseNonce: first.Nonce})
			cacheRequest()
			waitForWatches(t, c, 1)
			publish("rejected")
			rejected := receive(t, s)

			// Plain NACK: parked.
			sendAndObserve(t, s, observed, nack([]string{"a"}, rejected.Nonce))
			cacheRequest()
			waitForWatches(t, c, 1)
			select {
			case got := <-s.sent:
				t.Fatalf("rejected version resent on a plain NACK: %v", got)
			case <-time.After(50 * time.Millisecond):
			}

			// Subscription change while rejected: the client adds b and, as Envoy
			// does, still carries the error detail and the rejected nonce.
			sendAndObserve(t, s, observed, nack([]string{"a", "b"}, rejected.Nonce))
			cacheRequest()
			answered := receive(t, s)
			if answered.VersionInfo != "rejected" {
				t.Fatalf("subscription change while rejected must be answered at the rejected version, got %q", answered.VersionInfo)
			}
			if len(answered.Resources) == 0 {
				t.Fatal("subscription change while rejected must carry the newly subscribed resource")
			}

			// The client rejects that too; nothing further is sent.
			sendAndObserve(t, s, observed, nack([]string{"a", "b"}, answered.Nonce))
			cacheRequest()
			waitForWatches(t, c, 1)
			select {
			case got := <-s.sent:
				t.Fatalf("rejected version resent after the subscription change was rejected: %v", got)
			case <-time.After(50 * time.Millisecond):
			}

			publish("corrected")
			corrected := receive(t, s)
			if corrected.VersionInfo != "corrected" {
				t.Fatalf("unexpected correction: %v", corrected)
			}
		})
	}
}
