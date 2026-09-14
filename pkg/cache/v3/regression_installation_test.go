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
	"errors"
	"testing"
	"time"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	rsrc "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/stream/v3"
)

func TestInstallationSurvivesPartialResponseFailure(t *testing.T) {
	c := cache.NewSnapshotCache(true, kgwHash{}, nil)
	cds := make(chan cache.Response)
	eds := make(chan cache.Response)
	for typ, ch := range map[string]chan cache.Response{rsrc.ClusterType: cds, rsrc.EndpointType: eds} {
		cancel, err := c.CreateWatch(&discovery.DiscoveryRequest{TypeUrl: typ}, stream.NewSotwSubscription(nil, true), ch)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(cancel)
	}
	snap, err := cache.NewSnapshot("v2", map[rsrc.Type][]types.Resource{rsrc.ClusterType: {}, rsrc.EndpointType: {cla("a", 1)}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- c.SetSnapshot(ctx, kgwNode, snap) }()
	expectResponse(t, cds, "CDS is ordered before EDS")
	// No EDS receiver: cancellation is the only ready response-select branch.
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want cancellation after partial response, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("SetSnapshot did not observe cancellation")
	}
	got, err := c.GetSnapshot(kgwNode)
	if err != nil || got != snap {
		t.Fatalf("cache installation rolled back: snapshot=%v error=%v", got, err)
	}
	if n := c.GetStatusInfo(kgwNode).GetNumWatches(); n != 1 {
		t.Fatalf("want unserved EDS watch retained, got %d", n)
	}
	expectSilence(t, eds, "EDS was not delivered")
}

type observedSnapshot struct {
	cache.ResourceSnapshot
	entered chan struct{}
}

func (s observedSnapshot) GetResourcesAndTTL(typ string) map[string]types.ResourceWithTTL {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	return s.ResourceSnapshot.GetResourcesAndTTL(typ)
}

func TestFullImmediateChannelDoesNotBlockOtherNode(t *testing.T) {
	c := cache.NewSnapshotCache(true, kgwHash{}, nil)
	entered := make(chan struct{}, 1)
	snap := observedSnapshot{kgwEDS(t, "v1", cla("a", 1)), entered}
	if err := c.SetSnapshot(context.Background(), kgwNode, snap); err != nil {
		t.Fatal(err)
	}
	full := make(chan cache.Response, 1)
	full <- nil
	done := make(chan error, 1)
	go func() {
		_, err := c.CreateWatch(&discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}}, stream.NewSotwSubscription([]string{"a"}, false), full)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("CreateWatch never entered snapshot read")
	}
	other := make(chan error, 1)
	otherSnap := kgwEDS(t, "v9", cla("z", 1))
	go func() { other <- c.SetSnapshot(context.Background(), "another-node", otherSnap) }()
	// The unrelated node completes while the first response channel stays full.
	select {
	case err := <-other:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Error("unrelated node blocked behind the full response channel")
	}
	<-full
	select {
	case err := <-done:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Fatal("CreateWatch did not recover after drain")
	}
	expectResponse(t, full, "immediate response after drain")
}

func (s observedSnapshot) GetResources(typ string) map[string]types.Resource {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	return s.ResourceSnapshot.GetResources(typ)
}

func TestFullImmediateDeltaChannelDoesNotBlockOtherNode(t *testing.T) {
	c := cache.NewSnapshotCache(true, kgwHash{}, nil)
	entered := make(chan struct{}, 1)
	if err := c.SetSnapshot(context.Background(), kgwNode, observedSnapshot{kgwEDS(t, "v1", cla("a", 1)), entered}); err != nil {
		t.Fatal(err)
	}
	full := make(chan cache.DeltaResponse, 1)
	full <- nil
	done := make(chan error, 1)
	go func() {
		_, err := c.CreateDeltaWatch(&discovery.DeltaDiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNamesSubscribe: []string{"a"}}, stream.NewDeltaSubscription([]string{"a"}, nil, nil, false), full)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("delta watch never read the snapshot")
	}
	other := make(chan error, 1)
	otherSnap := kgwEDS(t, "v2", cla("z", 1))
	go func() { other <- c.SetSnapshot(context.Background(), "other", otherSnap) }()
	select {
	case err := <-other:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Error("unrelated node blocked behind delta response")
	}
	<-full
	select {
	case err := <-done:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Fatal("delta watch did not finish after draining")
	}
	select {
	case <-full:
	case <-time.After(time.Second):
		t.Fatal("missing immediate delta response")
	}
}

func TestCancelBlockedSnapshotSend(t *testing.T) {
	for _, delta := range []bool{false, true} {
		name := "sotw"
		if delta {
			name = "delta"
		}
		t.Run(name, func(t *testing.T) {
			c := cache.NewSnapshotCache(true, kgwHash{}, nil)
			var watchCancel func()
			var err error
			if delta {
				ch := make(chan cache.DeltaResponse, 1)
				ch <- nil
				watchCancel, err = c.CreateDeltaWatch(&discovery.DeltaDiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNamesSubscribe: []string{"a"}}, stream.NewDeltaSubscription([]string{"a"}, nil, nil, false), ch)
			} else {
				ch := make(chan cache.Response, 1)
				ch <- nil
				watchCancel, err = c.CreateWatch(&discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}}, stream.NewSotwSubscription([]string{"a"}, false), ch)
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(watchCancel)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			entered := make(chan struct{}, 1)
			snapshot := observedSnapshot{kgwEDS(t, "v1", cla("a", 1)), entered}
			done := make(chan error, 1)
			go func() { done <- c.SetSnapshot(ctx, kgwNode, snapshot) }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("snapshot did not evaluate watch")
			}
			other := make(chan error, 1)
			otherSnap := kgwEDS(t, "v2", cla("z", 1))
			go func() { other <- c.SetSnapshot(ctx, "other", otherSnap) }()
			select {
			case err := <-other:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("blocked response held cache lock")
			}
			c.ClearSnapshot(kgwNode)
			if c.GetStatusInfo(kgwNode) == nil {
				t.Fatal("clear discarded in-flight watch status")
			}
			watchCancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("watch cancellation did not interrupt send")
			}
			if c.GetStatusInfo(kgwNode) != nil {
				t.Fatal("canceled in-flight watch retained cleared status")
			}
		})
	}
}

func TestWaitingSnapshotCanCancelAndRetry(t *testing.T) {
	c := cache.NewSnapshotCache(true, kgwHash{}, nil)
	responses := make(chan cache.Response)
	watchCancel, err := c.CreateWatch(&discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}}, stream.NewSotwSubscription([]string{"a"}, false), responses)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(watchCancel)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	entered := make(chan struct{}, 1)
	first := observedSnapshot{kgwEDS(t, "v1", cla("a", 1)), entered}
	done := make(chan error, 1)
	go func() { done <- c.SetSnapshot(ctx, kgwNode, first) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first publication did not evaluate watch")
	}
	waiting, stop := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer stop()
	if err := c.SetSnapshot(waiting, kgwNode, kgwEDS(t, "v2", cla("a", 2))); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting publication did not cancel: %v", err)
	}
	if got, err := c.GetSnapshot(kgwNode); err != nil || got != first {
		t.Fatal("waiting publication overtook the pending response")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected canceled send, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first publication did not cancel")
	}
	if c.GetStatusInfo(kgwNode).GetNumWatches() != 1 {
		t.Fatal("failed send did not restore the watch")
	}
	third := kgwEDS(t, "v3", cla("a", 3))
	go func() { done <- c.SetSnapshot(context.Background(), kgwNode, third) }()
	got := expectResponse(t, responses, "retry answers restored watch")
	if got.GetResponseVersion() != "v3" {
		t.Fatalf("retry sent version %q", got.GetResponseVersion())
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("retry did not finish")
	}
}

func TestBlockedHeartbeatDoesNotBlockOtherNode(t *testing.T) {
	ctx := t.Context()
	c := cache.NewSnapshotCacheWithHeartbeating(ctx, true, kgwHash{}, nil, 10*time.Millisecond)
	ttl := time.Second
	snap, err := cache.NewSnapshotWithTTLs("v1", map[rsrc.Type][]types.ResourceWithTTL{rsrc.EndpointType: {{Resource: cla("a", 1), TTL: &ttl}}})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 8)
	if err := c.SetSnapshot(ctx, kgwNode, observedSnapshot{snap, entered}); err != nil {
		t.Fatal(err)
	}
	full := make(chan cache.Response, 1)
	full <- nil
	sub := stream.NewSotwSubscription([]string{"a"}, false)
	sub.SetReturnedResources(map[string]string{"a": "v1"})
	watchCancel, err := c.CreateWatch(&discovery.DiscoveryRequest{TypeUrl: rsrc.EndpointType, ResourceNames: []string{"a"}, VersionInfo: "v1"}, sub, full)
	if err != nil {
		t.Fatal(err)
	}
	defer watchCancel()
	// First read is CreateWatch; the next read is the heartbeat preparation.
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("heartbeat did not evaluate parked watch")
		}
	}
	other := make(chan error, 1)
	otherSnap := kgwEDS(t, "v2", cla("z", 1))
	go func() { other <- c.SetSnapshot(ctx, "other", otherSnap) }()
	select {
	case err := <-other:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("heartbeat held the cache lock during send")
	}
	watchCancel()
}
