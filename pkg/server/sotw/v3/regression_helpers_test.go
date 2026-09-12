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
	"testing"

	envoycorev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoyendpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	rsrc "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
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
