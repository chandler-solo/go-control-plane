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
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	rsrc "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/sotw/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/v3"
)

// A proxy reconnecting to a control plane that has just restarted: the cache
// is empty, the proxy's first endpoint request names the version it accepted
// on its old stream, and the control plane then republishes the same content
// under the same version. The republish must answer; before this change the
// watch stayed parked because the versions were equal.
func TestReconnectBeforeFirstSnapshotIsAnsweredAtHeldVersion(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		name := map[bool]string{false: "default-ads", true: "ordered-ads"}[ordered]
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			c := cache.NewSnapshotCache(true, kgwHash{}, nil)
			srv := server.NewServer(ctx, c, server.CallbackFuncs{})
			if ordered {
				srv = server.NewServer(ctx, c, server.CallbackFuncs{}, sotw.WithOrderedADS())
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
			select {
			case s.recv <- &discovery.DiscoveryRequest{Node: &envoycorev3.Node{Id: kgwNode}, TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}, VersionInfo: "v1"}:
			case <-time.After(time.Second):
				t.Fatal("request not consumed")
			}
			waitForNodeWatches(t, c, 1)
			select {
			case got := <-s.sent:
				t.Fatalf("nothing to answer with yet: %v", got)
			case <-time.After(50 * time.Millisecond):
			}
			if err := c.SetSnapshot(ctx, kgwNode, kgwEDS(t, "v1", cla("a", 1))); err != nil {
				t.Fatal(err)
			}
			got := receive(t, s)
			if got.VersionInfo != "v1" || len(got.Resources) != 1 {
				t.Fatalf("republish at the held version must answer the reconnecting proxy, got %v", got)
			}
			// The stream continues normally: an ACK parks, the next version is sent.
			select {
			case s.recv <- &discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}, VersionInfo: got.VersionInfo, ResponseNonce: got.Nonce}:
			case <-time.After(time.Second):
				t.Fatal("ACK not consumed")
			}
			waitForWatches(t, c, 1)
			if err := c.SetSnapshot(ctx, kgwNode, kgwEDS(t, "v1", cla("a", 1))); err != nil {
				t.Fatal(err)
			}
			select {
			case extra := <-s.sent:
				t.Fatalf("equal-version republish after the first answer must stay silent: %v", extra)
			case <-time.After(50 * time.Millisecond):
			}
			if err := c.SetSnapshot(ctx, kgwNode, kgwEDS(t, "v2", cla("a", 2))); err != nil {
				t.Fatal(err)
			}
			if next := receive(t, s); next.VersionInfo != "v2" {
				t.Fatalf("expected v2, got %v", next)
			}
		})
	}
}

// waitForNodeWatches is waitForWatches for a node that may have no status yet:
// before the server has processed the stream's first request there is nothing
// to count, and the shared helper would dereference a nil status.
func waitForNodeWatches(t *testing.T, c cache.SnapshotCache, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if info := c.GetStatusInfo(kgwNode); info != nil && info.GetNumWatches() == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("node never reached %d open watch(es)", want)
		}
		time.Sleep(time.Millisecond)
	}
}
