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

func TestClearSnapshotOrphansParkedWatchOnPin(t *testing.T) {
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
	expectSilence(t, ch, "equal-version request parks")
	if n := c.GetStatusInfo(kgwNode).GetNumWatches(); n != 1 {
		t.Fatalf("expected 1 parked watch, got %d", n)
	}

	c.ClearSnapshot(kgwNode)
	if info := c.GetStatusInfo(kgwNode); info != nil {
		t.Fatalf("status survived ClearSnapshot with %d watches", info.GetNumWatches())
	}
	expectSilence(t, ch, "ClearSnapshot does not answer or close the parked watch")

	// A new snapshot for the node has no memory of the parked watch.
	if err := c.SetSnapshot(ctx, kgwNode, kgwEDS(t, "v2", cla("a", 2))); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-ch:
		t.Fatalf("orphaned watch was answered (%v); watch retention changed, update this expectation", got.GetReturnedResources())
	case <-time.After(300 * time.Millisecond):
	}
	// SetSnapshot creates no status of its own; only a request does.
	if info := c.GetStatusInfo(kgwNode); info != nil && info.GetNumWatches() != 0 {
		t.Fatalf("new status carries %d watches; expected the old waiter to be orphaned", info.GetNumWatches())
	}
	// The server-side recovery path is a new request from the client.
	ch2 := make(chan cache.Response, 1)
	cancel2, err := c.CreateWatch(&discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}, VersionInfo: "v1"}, sub, ch2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cancel2)
	expectResponse(t, ch2, "a fresh request sees v2")
}
