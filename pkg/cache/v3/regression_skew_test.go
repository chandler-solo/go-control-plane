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
