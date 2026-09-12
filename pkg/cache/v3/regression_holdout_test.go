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

package cache_test

import (
	"context"
	"testing"
	"time"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	rsrc "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/stream/v3"
)

func TestClearSnapshotRetainsParkedWatch(t *testing.T) {
	ctx := context.Background()
	c := cache.NewSnapshotCache(true, kgwHash{}, nil)
	if err := c.SetSnapshot(ctx, kgwNode, kgwEDS(t, "v1", cla("a", 1))); err != nil {
		t.Fatal(err)
	}
	sub := stream.NewSotwSubscription([]string{"a"}, false)
	sub.SetReturnedResources(map[string]string{"a": "v1"})
	ch := make(chan cache.Response, 1)
	cancel, err := c.CreateWatch(&discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}, VersionInfo: "v1"}, sub, ch)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cancel)
	c.ClearSnapshot(kgwNode)
	if _, err := c.GetSnapshot(kgwNode); err == nil {
		t.Fatal("cleared snapshot is still available")
	}
	if info := c.GetStatusInfo(kgwNode); info == nil || info.GetNumWatches() != 1 {
		t.Fatalf("parked watch lost on clear: %v", info)
	}
	expectSilence(t, ch, "clear does not answer or close the watch")
	if err := c.SetSnapshot(ctx, kgwNode, kgwEDS(t, "v2", cla("a", 2))); err != nil {
		t.Fatal(err)
	}
	got := expectResponse(t, ch, "next snapshot answers the retained watch")
	if got.GetResponseVersion() != "v2" || len(got.GetReturnedResources()) != 1 {
		t.Fatalf("unexpected response: %v", got)
	}
	if _, ok := got.GetReturnedResources()["a"]; !ok {
		t.Fatal("response omitted a")
	}
	cancel()
	if info := c.GetStatusInfo(kgwNode); info == nil || info.GetNumWatches() != 0 {
		t.Fatalf("republished node status was removed or watch was retained: %v", info)
	}
	c.ClearSnapshot(kgwNode)
	if info := c.GetStatusInfo(kgwNode); info != nil {
		t.Fatal("clear retained status with no watches")
	}
}

func TestClearSnapshotRetainsDeltaWatch(t *testing.T) {
	c := cache.NewSnapshotCache(true, kgwHash{}, nil)
	ch := make(chan cache.DeltaResponse, 1)
	cancel, err := c.CreateDeltaWatch(&discovery.DeltaDiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNamesSubscribe: []string{"a"}}, stream.NewDeltaSubscription([]string{"a"}, nil, nil, false), ch)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cancel)
	c.ClearSnapshot(kgwNode)
	if info := c.GetStatusInfo(kgwNode); info == nil || info.GetNumDeltaWatches() != 1 {
		t.Fatalf("delta watch lost on clear: %v", info)
	}
	if err := c.SetSnapshot(context.Background(), kgwNode, kgwEDS(t, "v2", cla("a", 2))); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-ch:
		version, err := got.GetSystemVersion()
		if err != nil {
			t.Fatal(err)
		}
		if version != "v2" || len(got.GetNextVersionMap()) != 1 {
			t.Fatalf("unexpected delta response: %v", got)
		}
		if _, ok := got.GetNextVersionMap()["a"]; !ok {
			t.Fatal("delta response omitted a")
		}
	case <-time.After(time.Second):
		t.Fatal("delta watch was not answered after clear")
	}
}

func TestClearSnapshotRemovesStatusAfterLastCancellation(t *testing.T) {
	for _, deltaFirst := range []bool{false, true} {
		name := "sotw-first"
		if deltaFirst {
			name = "delta-first"
		}
		t.Run(name, func(t *testing.T) {
			c := cache.NewSnapshotCache(true, kgwHash{}, nil)
			sotwCancel, err := c.CreateWatch(&discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}}, stream.NewSotwSubscription([]string{"a"}, false), make(chan cache.Response, 1))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(sotwCancel)
			deltaCancel, err := c.CreateDeltaWatch(&discovery.DeltaDiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNamesSubscribe: []string{"a"}}, stream.NewDeltaSubscription([]string{"a"}, nil, nil, false), make(chan cache.DeltaResponse, 1))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(deltaCancel)
			c.ClearSnapshot(kgwNode)
			first, last := sotwCancel, deltaCancel
			if deltaFirst {
				first, last = last, first
			}
			first()
			info := c.GetStatusInfo(kgwNode)
			if info == nil || info.GetNumWatches()+info.GetNumDeltaWatches() != 1 {
				t.Fatalf("first cancellation lost remaining watch: %v", info)
			}
			last()
			if c.GetStatusInfo(kgwNode) != nil {
				t.Fatal("last cancellation retained cleared status")
			}
			// Cancellation is idempotent, including after a fresh status is created.
			fresh, err := c.CreateWatch(&discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}}, stream.NewSotwSubscription([]string{"a"}, false), make(chan cache.Response, 1))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(fresh)
			first()
			last()
			if info := c.GetStatusInfo(kgwNode); info == nil || info.GetNumWatches() != 1 {
				t.Fatalf("old cancellation removed new watch: %v", info)
			}
		})
	}
}
