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
	stream "github.com/envoyproxy/go-control-plane/pkg/server/stream/v3"
)

func TestUnsubscribeAllReceivesNothingFromCache(t *testing.T) {
	for _, entry := range []string{"immediate", "parked"} {
		t.Run(entry, func(t *testing.T) {
			c := cache.NewSnapshotCache(true, kgwHash{}, nil)
			initial := "v1"
			if entry == "immediate" {
				initial = "v2"
			}
			if err := c.SetSnapshot(context.Background(), kgwNode, kgwEDS(t, initial, cla("a", 1))); err != nil {
				t.Fatal(err)
			}
			sub := stream.NewSotwSubscription([]string{"a"}, true)
			sub.SetReturnedResources(map[string]string{"a": "v1"})
			sub.SetResourceSubscription(nil)
			if sub.IsWildcard() || len(sub.SubscribedResources()) != 0 {
				t.Fatal("fixture did not unsubscribe all")
			}
			ch := make(chan cache.Response, 1)
			cancel, err := c.CreateWatch(&discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, VersionInfo: "v1"}, sub, ch)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(cancel)
			expectSilence(t, ch, "unsubscribe-all is answered on the "+entry+" path: the v0.14.0 leak is back")
			if n := c.GetStatusInfo(kgwNode).GetNumWatches(); n != 0 {
				t.Fatalf("unsubscribe-all registered %d watch(es); the pin registers none", n)
			}
			if err = c.SetSnapshot(context.Background(), kgwNode, kgwEDS(t, "v3", cla("a", 2))); err != nil {
				t.Fatal(err)
			}
			expectSilence(t, ch, "a later snapshot answers an unsubscribed watch: the v0.14.0 leak is back")
		})
	}
}
