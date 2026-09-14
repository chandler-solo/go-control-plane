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

	envoycorev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	rsrc "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	sotw "github.com/envoyproxy/go-control-plane/pkg/server/sotw/v3"
	server "github.com/envoyproxy/go-control-plane/pkg/server/v3"
)

func TestUnsubscribeAllLeaksNothingOnWire(t *testing.T) {
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
			s := &scriptedStream{ctx: ctx, recv: make(chan *discovery.DiscoveryRequest), sent: make(chan *discovery.DiscoveryResponse)}
			srv := server.NewServer(ctx, c, server.CallbackFuncs{})
			if ordered {
				srv = server.NewServer(ctx, c, server.CallbackFuncs{}, sotw.WithOrderedADS())
			}
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
			first := s.exchange(t, &discovery.DiscoveryRequest{Node: &envoycorev3.Node{Id: kgwNode}, TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}})
			select {
			case s.recv <- &discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, VersionInfo: first.VersionInfo, ResponseNonce: first.Nonce}:
			case <-time.After(time.Second):
				t.Fatal("unsubscribe request not consumed")
			}
			// The server processes the request asynchronously; give it a bounded
			// window in which a v0.14.0-style parked watch would have appeared.
			deadline := time.Now().Add(300 * time.Millisecond)
			for time.Now().Before(deadline) {
				if n := c.GetStatusInfo(kgwNode).GetNumWatches(); n != 0 {
					t.Fatalf("unsubscribe-all parked %d watch(es): the v0.14.0 leak path is back", n)
				}
				time.Sleep(5 * time.Millisecond)
			}
			if err := c.SetSnapshot(ctx, kgwNode, kgwEDS(t, "v2", cla("a", 2))); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-s.sent:
				t.Fatalf("unrequested response leaked after unsubscribe-all: %v", got)
			case <-time.After(500 * time.Millisecond):
			}
			// A resubscribe at the old accepted version is answered with the new content.
			select {
			case s.recv <- &discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}, VersionInfo: first.VersionInfo}:
			case <-time.After(time.Second):
				t.Fatal("resubscribe request not consumed")
			}
			select {
			case got := <-s.sent:
				if got.TypeUrl != rsrc.EndpointType || len(got.Resources) != 1 || got.VersionInfo != "v2" {
					t.Fatalf("unexpected resubscribe response: %v", got)
				}
			case <-time.After(time.Second):
				t.Fatal("resubscribe after unsubscribe-all was not answered")
			}
		})
	}
}
