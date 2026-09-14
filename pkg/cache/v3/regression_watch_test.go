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

	envoycorev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoyendpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	rsrc "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/stream/v3"
)

const kgwNode = "regression-node"

type kgwHash struct{}

func (kgwHash) ID(*envoycorev3.Node) string { return kgwNode }

func kgwEDS(t *testing.T, version string, class ...*envoyendpointv3.ClusterLoadAssignment) *cache.Snapshot {
	t.Helper()
	res := make([]types.Resource, 0, len(class))
	for _, c := range class {
		res = append(res, c)
	}
	snap, err := cache.NewSnapshot(version, map[rsrc.Type][]types.Resource{rsrc.EndpointType: res})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	return snap
}

func cla(name string, hosts int) *envoyendpointv3.ClusterLoadAssignment {
	c := &envoyendpointv3.ClusterLoadAssignment{ClusterName: name}
	for range hosts {
		c.Endpoints = append(c.Endpoints, &envoyendpointv3.LocalityLbEndpoints{})
	}
	return c
}

func expectResponse(t *testing.T, ch chan cache.Response, what string) cache.Response {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(time.Second):
		t.Fatalf("expected a response: %s", what)
	}
	return nil
}

func expectSilence(t *testing.T, ch chan cache.Response, what string) {
	t.Helper()
	select {
	case r := <-ch:
		t.Fatalf("expected no response (%s), got version %v", what, r.GetRequest().GetTypeUrl())
	default: // Cache calls above are synchronous; no response remains queued.
	}
}

func TestParkedNamedWatchIsRetainedOnDeclinedResponse(t *testing.T) {
	ctx := context.Background()
	c := cache.NewSnapshotCache(true /* ads */, kgwHash{}, nil)
	if err := c.SetSnapshot(ctx, kgwNode, kgwEDS(t, "v1", cla("a", 1))); err != nil {
		t.Fatal(err)
	}

	req := &discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}}
	sub := stream.NewSotwSubscription([]string{"a"}, false)

	ch1 := make(chan cache.Response, 1)
	if _, err := c.CreateWatch(req, sub, ch1); err != nil {
		t.Fatal(err)
	}
	first := expectResponse(t, ch1, "fresh named watch")
	ackResponse(first, &sub, req) // Envoy ACKs v1, has {a}

	ch2 := make(chan cache.Response, 1)
	cancel2, err := c.CreateWatch(req, sub, ch2)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel2()
	if n := c.GetStatusInfo(kgwNode).GetNumWatches(); n != 1 {
		t.Fatalf("expected 1 parked watch, got %d", n)
	}

	// v2 adds b (a cluster Envoy has not applied/accepted): ADS declines.
	if err := c.SetSnapshot(ctx, kgwNode, kgwEDS(t, "v2", cla("a", 1), cla("b", 1))); err != nil {
		t.Fatal(err)
	}
	expectSilence(t, ch2, "superset declined {a} watch against {a,b}")
	if n := c.GetStatusInfo(kgwNode).GetNumWatches(); n != 1 {
		t.Fatalf("declined watch not retained (%d open): the v0.14.0 discard defect is back", n)
	}

	// A converging snapshot (b gone, a changed) is delivered to the retained
	// watch; no new request is needed.
	if err := c.SetSnapshot(ctx, kgwNode, kgwEDS(t, "v3", cla("a", 2))); err != nil {
		t.Fatal(err)
	}
	got := expectResponse(t, ch2, "converging snapshot delivered to the retained watch")
	if v := got.GetResponseVersion(); v != "v3" {
		t.Fatalf("retained watch answered with version %q, want v3", v)
	}
	if _, ok := got.GetReturnedResources()["a"]; !ok || len(got.GetReturnedResources()) != 1 {
		t.Fatalf("retained watch answered with resources %v, want exactly a", got.GetReturnedResources())
	}
}

func TestDeclinedNewRequestRetainsWatch(t *testing.T) {
	ctx := context.Background()
	c := cache.NewSnapshotCache(true, kgwHash{}, nil)
	if err := c.SetSnapshot(ctx, kgwNode, kgwEDS(t, "v2", cla("a", 1), cla("b", 1))); err != nil {
		t.Fatal(err)
	}

	req := &discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}, VersionInfo: "v1"}
	sub := stream.NewSotwSubscription([]string{"a"}, false)
	ch := make(chan cache.Response, 1)
	cancel, err := c.CreateWatch(req, sub, ch)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cancel)
	expectSilence(t, ch, "version differs but superset declines")
	if n := c.GetStatusInfo(kgwNode).GetNumWatches(); n != 1 {
		t.Fatalf("declined request registered %d watches, want 1", n)
	}
	if err := c.SetSnapshot(ctx, kgwNode, kgwEDS(t, "v3", cla("a", 2))); err != nil {
		t.Fatal(err)
	}
	got := expectResponse(t, ch, "aligned snapshot answers the retained request")
	if got.GetResponseVersion() != "v3" || len(got.GetReturnedResources()) != 1 {
		t.Fatalf("unexpected retained-watch response: version %q resources %v", got.GetResponseVersion(), got.GetReturnedResources())
	}
	if _, ok := got.GetReturnedResources()["a"]; !ok {
		t.Fatalf("response omitted requested resource a: %v", got.GetReturnedResources())
	}
	if n := c.GetStatusInfo(kgwNode).GetNumWatches(); n != 0 {
		t.Fatalf("answered watch remains registered: %d watches", n)
	}
}

func TestEqualVersionRespondsForNewlySubscribedResource(t *testing.T) {
	ctx := context.Background()
	c := cache.NewSnapshotCache(true, kgwHash{}, nil)
	if err := c.SetSnapshot(ctx, kgwNode, kgwEDS(t, "v1", cla("a", 1), cla("b", 1))); err != nil {
		t.Fatal(err)
	}

	req := &discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a", "b"}}
	sub := stream.NewSotwSubscription([]string{"a", "b"}, false)
	ch1 := make(chan cache.Response, 1)
	if _, err := c.CreateWatch(req, sub, ch1); err != nil {
		t.Fatal(err)
	}
	first := expectResponse(t, ch1, "initial {a,b}")
	ackResponse(first, &sub, req) // acked v1 with {a,b} returned

	// Envoy drops b and later re-subscribes to it (e.g. a cluster removed and
	// re-added on its side) — but the snapshot never changed: version still v1.
	sub.SetResourceSubscription([]string{"a"})
	sub.SetReturnedResources(map[string]string{"a": "v1"}) // b no longer held by the client
	sub.SetResourceSubscription([]string{"a", "b"})
	ch2 := make(chan cache.Response, 1)
	if _, err := c.CreateWatch(req, sub, ch2); err != nil {
		t.Fatal(err)
	}
	r := expectResponse(t, ch2, "equal version, newly subscribed b present in snapshot")
	if _, ok := r.GetReturnedResources()["b"]; !ok {
		t.Fatalf("expected the newly subscribed resource b in the equal-version response, got %v", r.GetReturnedResources())
	}
}

func TestWildcardWatchIsNeverDeclined(t *testing.T) {
	ctx := context.Background()
	c := cache.NewSnapshotCache(true, kgwHash{}, nil)
	if err := c.SetSnapshot(ctx, kgwNode, kgwEDS(t, "v1", cla("a", 1))); err != nil {
		t.Fatal(err)
	}
	req := &discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType} // no names: wildcard
	sub := stream.NewSotwSubscription(nil, true)
	ch1 := make(chan cache.Response, 1)
	if _, err := c.CreateWatch(req, sub, ch1); err != nil {
		t.Fatal(err)
	}
	ackResponse(expectResponse(t, ch1, "wildcard initial"), &sub, req)
	ch2 := make(chan cache.Response, 1)
	cancel, err := c.CreateWatch(req, sub, ch2)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	if err := c.SetSnapshot(ctx, kgwNode, kgwEDS(t, "v2", cla("a", 1), cla("b", 1))); err != nil {
		t.Fatal(err)
	}
	expectResponse(t, ch2, "wildcard delivered despite the extra resource")
}

func ackResponse(resp cache.Response, sub *stream.Subscription, req *cache.Request) {
	sub.SetReturnedResources(resp.GetReturnedResources())
	req.VersionInfo = resp.GetResponseVersion()
}
