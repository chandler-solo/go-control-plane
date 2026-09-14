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

	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"

	envoyclusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	envoycorev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	rsrc "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	server "github.com/envoyproxy/go-control-plane/pkg/server/v3"
)

type scriptedStream struct {
	grpc.ServerStream
	ctx  context.Context
	recv chan *discovery.DiscoveryRequest
	sent chan *discovery.DiscoveryResponse
}

func (s *scriptedStream) Context() context.Context { return s.ctx }
func (s *scriptedStream) Recv() (*discovery.DiscoveryRequest, error) {
	select {
	case r := <-s.recv:
		return r, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *scriptedStream) Send(r *discovery.DiscoveryResponse) error {
	select {
	case s.sent <- r:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *scriptedStream) exchange(t *testing.T, r *discovery.DiscoveryRequest) *discovery.DiscoveryResponse {
	t.Helper()
	select {
	case s.recv <- r:
	case <-time.After(time.Second):
		t.Fatal("request not consumed")
	}
	select {
	case response := <-s.sent:
		return response
	case <-time.After(time.Second):
		t.Fatal("no response")
	}
	return nil
}

func TestRepeatedNackResendsSameVersionAndCorrectionRecovers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := cache.NewSnapshotCache(true, kgwHash{}, nil)
	publish := func(version string) {
		t.Helper()
		snap, err := cache.NewSnapshot(version, map[rsrc.Type][]types.Resource{rsrc.ClusterType: {&envoyclusterv3.Cluster{Name: "a"}}})
		if err != nil {
			t.Fatal(err)
		}
		if err = c.SetSnapshot(ctx, kgwNode, snap); err != nil {
			t.Fatal(err)
		}
	}
	publish("rejected")
	s := &scriptedStream{ctx: ctx, recv: make(chan *discovery.DiscoveryRequest), sent: make(chan *discovery.DiscoveryResponse)}
	done := make(chan error, 1)
	go func() { done <- server.NewServer(ctx, c, server.CallbackFuncs{}).StreamAggregatedResources(s) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("stream did not shut down")
		}
	})
	resp := s.exchange(t, &discovery.DiscoveryRequest{Node: &envoycorev3.Node{Id: kgwNode}, TypeUrl: rsrc.ClusterType})
	for i := range 32 {
		previous := resp.GetNonce()
		resp = s.exchange(t, &discovery.DiscoveryRequest{TypeUrl: rsrc.ClusterType, ResponseNonce: previous, ErrorDetail: &rpcstatus.Status{Code: 3, Message: "scripted rejection"}})
		if resp.GetVersionInfo() != "rejected" || resp.GetNonce() == previous {
			t.Fatalf("unexpected retry at %d: %v", i, resp)
		}
	}
	publish("corrected")
	resp = s.exchange(t, &discovery.DiscoveryRequest{TypeUrl: rsrc.ClusterType, ResponseNonce: resp.GetNonce(), ErrorDetail: &rpcstatus.Status{Code: 3, Message: "last rejection"}})
	if resp.GetVersionInfo() != "corrected" {
		t.Fatalf("new snapshot not delivered: %v", resp)
	}
}
