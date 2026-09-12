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

	envoyendpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	rsrc "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/stream/v3"
)

func TestUnrequestedBootstrapClusterCLAWithholdsEDSForOlderProxy(t *testing.T) {
	ctx := context.Background()
	c := cache.NewSnapshotCache(true, kgwHash{}, nil)
	// The newer control plane's snapshot: the proxy's cluster plus a CLA for a
	// bootstrap-only local cluster this proxy generation does not have.
	if err := c.SetSnapshot(ctx, kgwNode, kgwEDS(t, "v1", cla("a", 1), cla("local", 1))); err != nil {
		t.Fatal(err)
	}
	sub := stream.NewSotwSubscription([]string{"a"}, false)
	ch := make(chan cache.Response, 1)
	cancel, err := c.CreateWatch(&discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}}, sub, ch)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cancel)
	expectSilence(t, ch, "superset check declines {a} against {a, local}: the older proxy receives no endpoints at all")

	// A later revision that still carries the unrequested CLA is declined too;
	// the blackout persists across updates, not just at connect.
	if err = c.SetSnapshot(ctx, kgwNode, kgwEDS(t, "v2", cla("a", 2), cla("local", 1))); err != nil {
		t.Fatal(err)
	}
	expectSilence(t, ch, "a changed revision with the unrequested CLA is still declined")

	// The repaired shape: the snapshot carries only CLAs the proxy can request.
	if err = c.SetSnapshot(ctx, kgwNode, kgwEDS(t, "v3", cla("a", 3))); err != nil {
		t.Fatal(err)
	}
	ch2 := make(chan cache.Response, 1)
	cancel2, err := c.CreateWatch(&discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}}, sub, ch2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cancel2)
	got := expectResponse(t, ch2, "filtered snapshot answers the older proxy")
	if _, ok := got.GetReturnedResources()["a"]; !ok || len(got.GetReturnedResources()) != 1 {
		t.Fatalf("unexpected returned resources %v", got.GetReturnedResources())
	}
}

func TestSubscriptionFilteredResponses(t *testing.T) {
	for _, policy := range []string{"legacy-ads", "filtered-ads", "non-ads"} {
		for _, entry := range []string{"immediate", "parked"} {
			for _, wildcard := range []bool{false, true} {
				name := "named"
				if wildcard {
					name = "wildcard"
				}
				t.Run(policy+"/"+entry+"/"+name, func(t *testing.T) {
					var opts []cache.Option
					if policy != "non-ads" {
						opts = append(opts, cache.WithADS())
					}
					if policy == "filtered-ads" {
						opts = append(opts, cache.WithSubscriptionFilteredResponses())
					}
					c := cache.NewSnapshotCacheWithOptions(kgwHash{}, nil, opts...)
					names := []string{"a"}
					if wildcard {
						names = nil
					}
					sub := stream.NewSotwSubscription(names, wildcard)
					req := &discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: names}
					initial := "v2"
					if entry == "parked" {
						initial = "v1"
						req.VersionInfo = "v1"
						known := map[string]string{"a": "v1"}
						if wildcard {
							known["local"] = "v1"
						}
						sub.SetReturnedResources(known)
					}
					snapshot := kgwEDS(t, initial, cla("a", 1), cla("local", 1))
					if err := c.SetSnapshot(context.Background(), kgwNode, snapshot); err != nil {
						t.Fatal(err)
					}
					ch := make(chan cache.Response, 1)
					cancel, err := c.CreateWatch(req, sub, ch)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(cancel)
					if entry == "parked" {
						expectSilence(t, ch, "equal version parks")
						if err := c.SetSnapshot(context.Background(), kgwNode, kgwEDS(t, "v2", cla("a", 2), cla("local", 1))); err != nil {
							t.Fatal(err)
						}
					}
					if policy == "legacy-ads" && !wildcard {
						expectSilence(t, ch, "legacy constructor option retains superset policy")
						return
					}
					got := expectResponse(t, ch, "response for the subscription")
					wire, err := got.GetDiscoveryResponse()
					if err != nil {
						t.Fatal(err)
					}
					want := map[string]bool{"a": true}
					if wildcard {
						want["local"] = true
					}
					if wire.VersionInfo != "v2" || len(wire.Resources) != len(want) {
						t.Fatalf("unexpected response: %v", wire)
					}
					for _, resource := range wire.Resources {
						endpoint := &envoyendpointv3.ClusterLoadAssignment{}
						if err := resource.UnmarshalTo(endpoint); err != nil {
							t.Fatal(err)
						}
						if !want[endpoint.ClusterName] {
							t.Fatalf("unexpected or repeated resource %q", endpoint.ClusterName)
						}
						delete(want, endpoint.ClusterName)
					}
					if len(want) != 0 {
						t.Fatalf("missing resources: %v", want)
					}
					if len(snapshot.GetResources(rsrc.EndpointType)) != 2 {
						t.Fatal("filter mutated the shared snapshot")
					}
				})
			}
		}
	}
}

func TestSubscriptionFilteredResponsesAtEqualVersion(t *testing.T) {
	c := cache.NewSnapshotCacheWithOptions(kgwHash{}, nil, cache.WithSubscriptionFilteredResponses(), cache.WithADS())
	snapshot := kgwEDS(t, "v1", cla("a", 1), cla("b", 1), cla("local", 1))
	if err := c.SetSnapshot(context.Background(), kgwNode, snapshot); err != nil {
		t.Fatal(err)
	}
	sub := stream.NewSotwSubscription([]string{"a", "b"}, false)
	sub.SetReturnedResources(map[string]string{"a": "v1"})
	req := &discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a", "b"}, VersionInfo: "v1"}
	ch := make(chan cache.Response, 1)
	cancel, err := c.CreateWatch(req, sub, ch)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cancel)
	got := expectResponse(t, ch, "newly subscribed b is answered at equal version")
	wire, err := got.GetDiscoveryResponse()
	if err != nil {
		t.Fatal(err)
	}
	if wire.VersionInfo != "v1" || len(wire.Resources) != 1 {
		t.Fatalf("unexpected response: %v", wire)
	}
	endpoint := &envoyendpointv3.ClusterLoadAssignment{}
	if err := wire.Resources[0].UnmarshalTo(endpoint); err != nil {
		t.Fatal(err)
	}
	if endpoint.ClusterName != "b" {
		t.Fatalf("expected newly requested b, got %q", endpoint.ClusterName)
	}
	if len(got.GetReturnedResources()) != 2 {
		t.Fatalf("returned state lost a: %v", got.GetReturnedResources())
	}
	if len(snapshot.GetResources(rsrc.EndpointType)) != 3 {
		t.Fatal("filter mutated snapshot")
	}
	sub.SetResourceSubscription(nil)
	cancelEmpty, err := c.CreateWatch(&discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, VersionInfo: "v1"}, sub, ch)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cancelEmpty)
	expectSilence(t, ch, "unsubscribe-all sends nothing with filtering enabled")
	if c.GetStatusInfo(kgwNode).GetNumWatches() != 0 {
		t.Fatal("unsubscribe-all retained a watch")
	}
}
