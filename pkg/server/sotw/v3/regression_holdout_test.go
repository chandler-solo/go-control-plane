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
	server "github.com/envoyproxy/go-control-plane/pkg/server/v3"
)

func TestResubscribeAtEqualVersionIsAnsweredOnPin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := cache.NewSnapshotCache(true, kgwHash{}, nil)
	if err := c.SetSnapshot(ctx, kgwNode, kgwEDS(t, "v1", cla("a", 1))); err != nil {
		t.Fatal(err)
	}
	srv := server.NewServer(ctx, c, server.CallbackFuncs{})
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

	first := s.exchange(t, &discovery.DiscoveryRequest{Node: &envoycorev3.Node{Id: kgwNode}, TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}})
	// ACK, then unsubscribe from a at the accepted version.
	for _, names := range [][]string{{"a"}, {}} {
		select {
		case s.recv <- &discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: names, VersionInfo: first.GetVersionInfo(), ResponseNonce: first.GetNonce()}:
		case <-time.After(time.Second):
			t.Fatal("request not consumed")
		}
		waitForWatches(t, c, 1)
	}
	// Resubscribe to a at the same version: the protocol requires a resend.
	got := s.exchange(t, &discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}, VersionInfo: first.GetVersionInfo(), ResponseNonce: first.GetNonce()})
	if got.GetVersionInfo() != first.GetVersionInfo() || len(got.GetResources()) != 1 {
		t.Fatalf("resubscribe at equal version was not re-sent the resource: %v; the resubscription repair regressed", got)
	}
	if got.GetNonce() == first.GetNonce() {
		t.Fatal("resend reused the previous nonce")
	}
}
