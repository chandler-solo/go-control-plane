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

	envoyclusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	rsrc "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/stream/v3"
)

// A watch opened while the node has no snapshot carries whatever version the
// client accepted before, on an earlier stream or from an earlier control
// plane. The first snapshot must answer it even when its version equals that
// held version: nothing has been sent on this watch, and a control plane that
// republishes the same content under the same version after a restart would
// otherwise leave the client parked, which for a warming cluster means waiting
// on the client's own fetch timeout.
func TestFirstSnapshotAnswersWatchOpenedBeforeItRegardlessOfVersion(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		typ   string
		names []string
	}{
		{name: "named", typ: rsrc.EndpointType, names: []string{"a"}},
		{name: "wildcard", typ: rsrc.ClusterType},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := cache.NewSnapshotCache(true, kgwHash{}, nil)
			sub := stream.NewSotwSubscription(tc.names, tc.names == nil)
			ch := make(chan cache.Response, 1)
			cancel, err := c.CreateWatch(&discovery.DiscoveryRequest{TypeUrl: tc.typ, ResourceNames: tc.names, VersionInfo: "v1"}, sub, ch)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(cancel)
			expectSilence(t, ch, "no snapshot yet")

			if err := c.SetSnapshot(ctx, kgwNode, firstSnapshot(t, "v1")); err != nil {
				t.Fatal(err)
			}
			got := expectResponse(t, ch, "the first snapshot answers a watch opened before it even at the held version")
			if v := got.GetResponseVersion(); v != "v1" {
				t.Fatalf("answered with version %q, want v1", v)
			}
		})
	}
}

// The rule is scoped to watches opened before any snapshot. Once a snapshot
// exists, a wildcard request at its version still parks and an equal-version
// republish still sends nothing.
func TestEqualVersionStillParksWhenASnapshotExists(t *testing.T) {
	ctx := context.Background()
	c := cache.NewSnapshotCache(true, kgwHash{}, nil)
	if err := c.SetSnapshot(ctx, kgwNode, firstSnapshot(t, "v1")); err != nil {
		t.Fatal(err)
	}
	sub := stream.NewSotwSubscription(nil, true)
	sub.SetReturnedResources(map[string]string{"cluster": "v1"})
	ch := make(chan cache.Response, 1)
	cancel, err := c.CreateWatch(&discovery.DiscoveryRequest{TypeUrl: rsrc.ClusterType, VersionInfo: "v1"}, sub, ch)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cancel)
	expectSilence(t, ch, "equal version against an existing snapshot parks")
	if err := c.SetSnapshot(ctx, kgwNode, firstSnapshot(t, "v1")); err != nil {
		t.Fatal(err)
	}
	expectSilence(t, ch, "equal-version republish against a watch opened with a snapshot present")
	if err := c.SetSnapshot(ctx, kgwNode, firstSnapshot(t, "v2")); err != nil {
		t.Fatal(err)
	}
	expectResponse(t, ch, "a new version is delivered")
}

// firstSnapshot holds one cluster and its assignment, both at version.
func firstSnapshot(t *testing.T, version string) *cache.Snapshot {
	t.Helper()
	snap, err := cache.NewSnapshot(version, map[rsrc.Type][]types.Resource{
		rsrc.ClusterType:  {&envoyclusterv3.Cluster{Name: "cluster"}},
		rsrc.EndpointType: {cla("a", 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	return snap
}
