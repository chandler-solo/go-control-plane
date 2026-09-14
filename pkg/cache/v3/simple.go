// Copyright 2018 Envoyproxy Authors
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

package cache

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/log"
)

// ResourceSnapshot is an abstract snapshot of a collection of resources that
// can be stored in a SnapshotCache. This enables applications to use the
// SnapshotCache watch machinery with their own resource types. Most
// applications will use Snapshot.
type ResourceSnapshot interface {
	// GetVersion should return the current version of the resource indicated
	// by typeURL. The version string that is returned is opaque and should
	// only be compared for equality.
	GetVersion(typeURL string) string

	// GetResourcesAndTTL returns all resources of the type indicted by
	// typeURL, together with their TTL.
	GetResourcesAndTTL(typeURL string) map[string]types.ResourceWithTTL

	// GetResources returns all resources of the type indicted by
	// typeURL. This is identical to GetResourcesAndTTL, except that
	// the TTL is omitted.
	GetResources(typeURL string) map[string]types.Resource

	// ConstructVersionMap is a hint that a delta watch will soon make a
	// call to GetVersionMap. The snapshot should construct an internal
	// opaque version string for each collection of resource types.
	ConstructVersionMap() error

	// GetVersionMap returns a map of resource name to resource version for
	// all the resources of type indicated by typeURL.
	GetVersionMap(typeURL string) map[string]string
}

// SnapshotCache is a snapshot-based cache that maintains a single versioned
// snapshot of responses per node. SnapshotCache consistently replies with the
// latest snapshot. By default in ADS mode, EDS/RDS
// requests are responded only when all resources in the snapshot xDS response
// are named as part of the request. It is expected that the CDS response names
// all EDS clusters, and the LDS response names all RDS routes in a snapshot,
// to ensure that Envoy makes the request for all EDS clusters or RDS routes
// eventually. WithSubscriptionFilteredResponses instead answers named ADS
// requests with only their subscribed resources. That drops the wait the
// sentence above describes: a client that names a resource absent from the
// snapshot is answered without it and keeps waiting for that name, and a
// resource the client has not named is never sent to it. Delivery of a newly
// added EDS cluster or RDS route then relies on the client re-requesting with
// the new name once its CDS or LDS changes, which Envoy does.
//
// SnapshotCache can operate as a REST or regular xDS backend. The snapshot
// can be partial, e.g. only include RDS or EDS resources.
type SnapshotCache interface {
	Cache

	// SetSnapshot sets a response snapshot for a node. For ADS, the snapshots
	// should have distinct versions and be internally consistent (e.g. all
	// referenced resources must be included in the snapshot).
	//
	// This method will cause the server to respond to all open watches, for which
	// the version differs from the snapshot version.
	//
	// Contract: the snapshot is installed first and responses are sent after the
	// cache and node locks are released, serialized per node so ADS order is
	// kept. A returned error after installation (a canceled context or a
	// blocked response channel) means some responses were not sent; the
	// snapshot stays installed and the unsent watches stay registered, so the
	// next call answers them. A caller whose context is already canceled still
	// installs when no send is pending. Canceling a watch interrupts a send
	// blocked on its channel; a response already handed to the channel is not
	// withdrawn.
	SetSnapshot(ctx context.Context, node string, snapshot ResourceSnapshot) error

	// GetSnapshots gets the snapshot for a node.
	GetSnapshot(node string) (ResourceSnapshot, error)

	// ClearSnapshot removes the snapshot associated with a node. Status is retained
	// while watches are open so a later snapshot can answer them. To remove all
	// status immediately, cancel the node's streams before clearing its snapshot.
	ClearSnapshot(node string)

	// GetStatusInfo retrieves status information for a node ID.
	GetStatusInfo(string) StatusInfo

	// GetStatusKeys retrieves node IDs for all statuses.
	GetStatusKeys() []string
}

type snapshotCache struct {
	// watchCount and deltaWatchCount are atomic counters incremented for each watch respectively. They need to
	// be the first fields in the struct to guarantee 64-bit alignment,
	// which is a requirement for atomic operations on 64-bit operands to work on
	// 32-bit machines.
	watchCount      atomic.Int64
	deltaWatchCount atomic.Int64

	log log.Logger

	// ads flag to hold responses until all resources are named
	ads bool

	subscriptionFilteredResponses bool

	// snapshots are cached resources indexed by node IDs
	snapshots map[string]ResourceSnapshot

	// status information for all nodes indexed by node IDs
	status map[string]*statusInfo

	// hash is the hashing function for Envoy nodes
	hash NodeHash

	responseLocks map[string]*nodeResponseLock

	mu sync.RWMutex
}

// NewSnapshotCache initializes a simple cache.
//
// ADS flag forces a delay in responding to streaming requests until all
// resources are explicitly named in the request. This avoids the problem of a
// partial request over a single stream for a subset of resources which would
// require generating a fresh version for acknowledgement. ADS flag requires
// snapshot consistency. For non-ADS case (and fetch), multiple partial
// requests are sent across multiple streams and re-using the snapshot version
// is OK.
//
// Logger is optional.
func NewSnapshotCache(ads bool, hash NodeHash, logger log.Logger) SnapshotCache {
	return newSnapshotCache(ads, hash, logger)
}

// Option configures a snapshot cache created with NewSnapshotCacheWithOptions.
type Option func(*snapshotCache)

// WithADS enables ADS response ordering and the legacy named-response policy.
func WithADS() Option {
	return func(cache *snapshotCache) { cache.ads = true }
}

// WithSubscriptionFilteredResponses allows named ADS responses to contain only
// subscribed resources instead of waiting for every snapshot resource to be named.
// It has no effect on wildcard responses, non-ADS caches, fetches, or delta watches.
func WithSubscriptionFilteredResponses() Option {
	return func(cache *snapshotCache) { cache.subscriptionFilteredResponses = true }
}

// NewSnapshotCacheWithOptions initializes a snapshot cache with optional policies.
// By default ADS is disabled. Logger is optional.
func NewSnapshotCacheWithOptions(hash NodeHash, logger log.Logger, opts ...Option) SnapshotCache {
	cache := newSnapshotCache(false, hash, logger)
	for _, opt := range opts {
		opt(cache)
	}
	return cache
}

func newSnapshotCache(ads bool, hash NodeHash, logger log.Logger) *snapshotCache {
	if logger == nil {
		logger = log.NewDefaultLogger()
	}

	cache := &snapshotCache{
		log:       logger,
		ads:       ads,
		snapshots: make(map[string]ResourceSnapshot),
		status:    make(map[string]*statusInfo),
		hash:      hash,
	}

	return cache
}

// NewSnapshotCacheWithHeartbeating initializes a simple cache that sends periodic heartbeat
// responses for resources with a TTL.
//
// ADS flag forces a delay in responding to streaming requests until all
// resources are explicitly named in the request. This avoids the problem of a
// partial request over a single stream for a subset of resources which would
// require generating a fresh version for acknowledgement. ADS flag requires
// snapshot consistency. For non-ADS case (and fetch), multiple partial
// requests are sent across multiple streams and re-using the snapshot version
// is OK.
//
// Logger is optional.
//
// The context provides a way to cancel the heartbeating routine, while the heartbeatInterval
// parameter controls how often heartbeating occurs.
func NewSnapshotCacheWithHeartbeating(ctx context.Context, ads bool, hash NodeHash, logger log.Logger, heartbeatInterval time.Duration) SnapshotCache {
	cache := newSnapshotCache(ads, hash, logger)
	go func() {
		t := time.NewTicker(heartbeatInterval)
		defer t.Stop()

		for {
			select {
			case <-t.C:
				cache.mu.RLock()
				nodes := slices.Collect(maps.Keys(cache.status))
				cache.mu.RUnlock()
				for _, node := range nodes {
					cache.sendHeartbeats(ctx, node)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return cache
}

func (cache *snapshotCache) sendHeartbeats(ctx context.Context, node string) {
	unlock, err := cache.lockResponses(ctx, node)
	if err != nil {
		return
	}
	defer unlock()
	cache.mu.Lock()
	snapshot, exists := cache.snapshots[node]
	info := cache.status[node]
	if !exists || info == nil {
		cache.mu.Unlock()
		return
	}
	info.mu.Lock()
	pending := cache.collectHeartbeats(info, snapshot)
	info.responsesInFlight += len(pending)
	info.mu.Unlock()
	cache.mu.Unlock()
	if err := cache.sendResponses(ctx, node, info, pending, nil); err != nil {
		cache.log.Errorf("failed to send heartbeat responses: %v", err)
	}
}

func (cache *snapshotCache) collectHeartbeats(info *statusInfo, snapshot ResourceSnapshot) []pendingSotwResponse {
	var pending []pendingSotwResponse
	info.orderResponseWatches()
	for _, key := range info.orderedWatches {
		id := key.ID
		watch := info.watches[id]
		// Respond with the current version regardless of whether the version has changed.
		version := snapshot.GetVersion(watch.Request.GetTypeUrl())
		resources := snapshot.GetResourcesAndTTL(watch.Request.GetTypeUrl())

		resourcesToReturn := map[string]*cachedResource{}
		addResource := func(name string, res types.ResourceWithTTL) {
			if res.TTL == nil {
				return
			}
			if _, exists := resourcesToReturn[name]; exists {
				// Already added
				return
			}
			resourcesToReturn[name] = newCachedResourceWithTTL(name, res, version)
		}

		if watch.subscription.IsWildcard() {
			resourcesToReturn = make(map[string]*cachedResource, len(resources))
			for name, res := range resources {
				addResource(name, res)
			}
		}
		for name := range watch.subscription.SubscribedResources() {
			if res, ok := resources[name]; ok {
				addResource(name, res)
			}
		}

		if len(resourcesToReturn) == 0 {
			continue
		}

		resp := &RawResponse{
			Request:   watch.Request,
			Version:   version,
			resources: slices.Collect(maps.Values(resourcesToReturn)),
			// Do not alter it. Those TTLs do not touch what's actually in watches.
			returnedResources: maps.Clone(watch.subscription.ReturnedResources()),
			Heartbeat:         true,
		}

		pending = append(pending, pendingSotwResponse{id: id, watch: watch, response: resp})
		delete(info.watches, id)
	}
	return pending
}

// SetSnapshot updates a snapshot for a node.
func (cache *snapshotCache) SetSnapshot(ctx context.Context, node string, snapshot ResourceSnapshot) error {
	unlock, err := cache.lockResponses(ctx, node)
	if err != nil {
		return err
	}
	defer unlock()
	cache.mu.Lock()
	cache.log.Debugf("setting snapshot for node %s", node)
	cache.snapshots[node] = snapshot
	info, ok := cache.status[node]
	if !ok {
		cache.mu.Unlock()
		return nil
	}
	info.mu.Lock()
	info.snapshotCleared = false
	sotw := cache.collectSotwResponses(info, snapshot)
	delta, prepareErr := cache.collectDeltaResponses(info, snapshot)
	info.responsesInFlight += len(sotw) + len(delta)
	info.mu.Unlock()
	cache.mu.Unlock()
	if err := cache.sendResponses(ctx, node, info, sotw, delta); err != nil {
		return err
	}
	return prepareErr
}

func (cache *snapshotCache) collectSotwResponses(info *statusInfo, snapshot ResourceSnapshot) []pendingSotwResponse {
	var pending []pendingSotwResponse
	collect := func(id int64, watch ResponseWatch) {
		if !watch.answerFirstSnapshot && snapshot.GetVersion(watch.Request.GetTypeUrl()) == watch.Request.GetVersionInfo() {
			return
		}
		resp, declined := createResponse(snapshot, watch, cache.ads, cache.subscriptionFilteredResponses)
		if declined {
			cache.log.Debugf("ADS mode: holding open watch %d %s %v at version %q: the snapshot has resources the client has not named", id, watch.Request.GetTypeUrl(), watch.Request.GetResourceNames(), snapshot.GetVersion(watch.Request.GetTypeUrl()))
		}
		if resp != nil {
			pending = append(pending, pendingSotwResponse{id: id, watch: watch, response: resp})
			delete(info.watches, id)
		}
	}
	if cache.ads {
		info.orderResponseWatches()
		for _, key := range info.orderedWatches {
			collect(key.ID, info.watches[key.ID])
		}
	} else {
		for id, watch := range info.watches {
			collect(id, watch)
		}
	}
	return pending
}

func (cache *snapshotCache) collectDeltaResponses(info *statusInfo, snapshot ResourceSnapshot) ([]pendingDeltaResponse, error) {
	if len(info.deltaWatches) == 0 {
		return nil, nil
	}
	if err := snapshot.ConstructVersionMap(); err != nil {
		return nil, err
	}
	var pending []pendingDeltaResponse
	collect := func(id int64, watch DeltaResponseWatch) {
		if resp := createDeltaResponse(snapshot, watch, false); resp != nil {
			pending = append(pending, pendingDeltaResponse{id: id, watch: watch, response: resp})
			delete(info.deltaWatches, id)
		}
	}
	if cache.ads {
		info.orderResponseDeltaWatches()
		for _, key := range info.orderedDeltaWatches {
			collect(key.ID, info.deltaWatches[key.ID])
		}
	} else {
		for id, watch := range info.deltaWatches {
			collect(id, watch)
		}
	}
	return pending, nil
}

// GetSnapshot gets the snapshot for a node, and returns an error if not found.
func (cache *snapshotCache) GetSnapshot(node string) (ResourceSnapshot, error) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	snap, ok := cache.snapshots[node]
	if !ok {
		return nil, fmt.Errorf("no snapshot found for node %s", node)
	}
	return snap, nil
}

// ClearSnapshot clears a node's snapshot, retaining status for open watches.
func (cache *snapshotCache) ClearSnapshot(node string) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	delete(cache.snapshots, node)
	if info, ok := cache.status[node]; ok {
		info.mu.Lock()
		defer info.mu.Unlock()
		info.snapshotCleared = true
		cache.removeClearedStatus(node, info)
	}
}

// removeClearedStatus requires both cache.mu and info.mu to be held for writing.
func (cache *snapshotCache) removeClearedStatus(node string, info *statusInfo) {
	if info.snapshotCleared && info.responsesInFlight == 0 && len(info.watches) == 0 && len(info.deltaWatches) == 0 {
		delete(cache.status, node)
	}
}

// CreateWatch returns a watch for an xDS request.  A nil function may be
// returned if an error occurs.
func (cache *snapshotCache) CreateWatch(request *Request, sub Subscription, value chan Response) (func(), error) {
	cache.mu.Lock()
	cancel, watch, resp := cache.prepareWatch(request, sub, value)
	cache.mu.Unlock()
	if resp != nil {
		if err := cache.respond(context.Background(), watch, resp); err != nil {
			return nil, fmt.Errorf("failed to send the response: %w", err)
		}
	}
	return cancel, nil
}

// prepareWatch requires cache.mu to be held for writing.
func (cache *snapshotCache) prepareWatch(request *Request, sub Subscription, value chan Response) (func(), ResponseWatch, *RawResponse) {
	nodeID := cache.hash.ID(request.GetNode())

	info, ok := cache.status[nodeID]
	if !ok {
		info = newStatusInfo(request.GetNode())
		cache.status[nodeID] = info
	}

	// update last watch request time
	info.setLastWatchRequestTime(time.Now())

	createWatch := func(watch ResponseWatch) func() {
		done := make(chan struct{})
		watch.done = done
		watchID := cache.nextWatchID()
		cache.log.Debugf("open watch %d for %s %v from nodeID %q, version %q", watchID, request.GetTypeUrl(), sub.SubscribedResources(), nodeID, request.GetVersionInfo())
		info.mu.Lock()
		info.watches[watchID] = watch
		info.mu.Unlock()
		return cache.cancelWatch(nodeID, watchID, done)
	}

	if !sub.IsWildcard() && len(sub.SubscribedResources()) == 0 {
		return func() {}, ResponseWatch{}, nil
	}

	watch := ResponseWatch{Request: request, Response: value, subscription: sub, fullStateResponses: ResourceRequiresFullStateInSotw(request.GetTypeUrl())}

	snapshot, exists := cache.snapshots[nodeID]
	if !exists {
		// No snapshot to compare against. Whatever version the request names
		// was accepted on a previous stream or from a previous control plane;
		// the first snapshot for this node must answer regardless of it.
		watch.answerFirstSnapshot = true
		return createWatch(watch), watch, nil
	}

	resp, declined := createResponse(snapshot, watch, cache.ads, cache.subscriptionFilteredResponses)
	if declined {
		cache.log.Debugf("ADS mode: retaining watch for request %s %v: the snapshot has resources the client has not named", request.GetTypeUrl(), request.GetResourceNames())
	}
	if resp != nil {
		return func() {}, watch, resp
	}

	return createWatch(watch), watch, nil
}

func (cache *snapshotCache) nextWatchID() int64 {
	return cache.watchCount.Add(1)
}

// cancellation function for cleaning stale watches.
func (cache *snapshotCache) cancelWatch(nodeID string, watchID int64, done chan struct{}) func() {
	var once sync.Once
	return func() {
		once.Do(func() { close(done) })
		// The common case only needs the read lock: cancels must not serialize
		// against every SetSnapshot for the sake of the rare cleared node.
		cache.mu.RLock()
		var idleCleared bool
		if info, ok := cache.status[nodeID]; ok {
			info.mu.Lock()
			delete(info.watches, watchID)
			idleCleared = info.snapshotCleared && len(info.watches) == 0 && len(info.deltaWatches) == 0
			info.mu.Unlock()
		}
		cache.mu.RUnlock()
		if idleCleared {
			cache.dropClearedStatus(nodeID)
		}
	}
}

// dropClearedStatus removes a node's status if it is still cleared and has no
// watches. It re-checks under the write lock because a SetSnapshot or a new
// watch can arrive between a cancel's read-locked check and this call; either
// makes the status live again and it must stay.
func (cache *snapshotCache) dropClearedStatus(nodeID string) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if info, ok := cache.status[nodeID]; ok {
		info.mu.Lock()
		cache.removeClearedStatus(nodeID, info)
		info.mu.Unlock()
	}
}

// difference returns the names present in resources but not in names.
func difference[T any](resources map[string]types.ResourceWithTTL, names map[string]T) []string {
	var diff []string
	for resourceName := range resources {
		if _, exists := names[resourceName]; !exists {
			diff = append(diff, resourceName)
		}
	}
	return diff
}

// createResponse evaluates the provided watch against the given snapshot to build the response to return.
// It may return a nil response to indicate the watch is up-to-date for the given snapshot.
// It is currently inefficient as not evaluating known resources intrisic versions, but only the snapshot one.
// Further work may be performed to optimize this.
// createResponse returns a nil response with declined set when the legacy ADS
// policy holds the response because the snapshot contains resources the client
// has not named; callers log that so a withheld named type stays visible.
func createResponse(snapshot ResourceSnapshot, watch ResponseWatch, ads, subscriptionFilteredResponses bool) (resp *RawResponse, declined bool) {
	typeURL := watch.Request.TypeUrl
	resources := snapshot.GetResourcesAndTTL(typeURL)

	subscribedResources := watch.subscription.SubscribedResources()
	if !watch.subscription.IsWildcard() && ads {
		if subscriptionFilteredResponses {
			filtered := make(map[string]types.ResourceWithTTL, len(subscribedResources))
			for name := range subscribedResources {
				if resource, ok := resources[name]; ok {
					filtered[name] = resource
				}
			}
			resources = filtered
		} else {
			// Preserve the legacy ADS policy until filtering is explicitly enabled.
			for name := range resources {
				if _, ok := subscribedResources[name]; !ok {
					return nil, true
				}
			}
		}
	}

	// This implementation can seem more complex than needed, as it does not blindly rely on the request version.
	// This allows for a more generic implemenentation when considering wildcard + subscribed, or partial replies.

	reqVersion := watch.Request.VersionInfo
	if watch.answerFirstSnapshot {
		// See ResponseWatch.answerFirstSnapshot: the held version is not one
		// this cache sent, so it does not make the watch up to date.
		reqVersion = ""
	}
	version := snapshot.GetVersion(typeURL)

	knownResources := watch.subscription.ReturnedResources()

	// Only populated if version has not changed.
	var changedResources map[string]struct{} // Use a map to merge wildcard and subscription.
	var deletedResources []string

	if version == reqVersion {
		// Check if a resource was not previously returned (e.g. if the watch is newly wildcard).
		if watch.subscription.IsWildcard() {
			changedResources = make(map[string]struct{}, len(resources))
			for name := range resources {
				_, known := knownResources[name]
				if known {
					continue
				}
				changedResources[name] = struct{}{}
			}
		} else {
			changedResources = make(map[string]struct{}, len(subscribedResources))
		}

		for name := range subscribedResources {
			_, exist := resources[name]
			if !exist {
				continue
			}
			_, known := knownResources[name]
			if known {
				continue
			}
			changedResources[name] = struct{}{}
		}

		if len(changedResources) == 0 && !watch.sendFullStateResponses() {
			// If full state responses are needed we need to trigger if only deletions occurred,
			// otherwise we can just bail out.
			return nil, false
		}

		for name := range knownResources {
			_, exist := resources[name]
			if !exist {
				deletedResources = append(deletedResources, name)
				continue
			}

			if watch.subscription.IsWildcard() {
				continue
			}
			_, watched := subscribedResources[name]
			if !watched {
				// Resource is no longer watched.
				deletedResources = append(deletedResources, name)
				continue
			}
		}

		if len(changedResources) == 0 && len(deletedResources) == 0 {
			// Nothing's changed
			return nil, false
		}
	}

	// Now compute the response.
	var resourcesToReturn []*cachedResource
	var returnedResources map[string]string
	if version != reqVersion || watch.sendFullStateResponses() {
		// Return all resources, with no regard to known version.
		if watch.subscription.IsWildcard() {
			resourcesToReturn = make([]*cachedResource, 0, len(resources))
			returnedResources = make(map[string]string, len(resources))
			for name, resource := range resources {
				resourcesToReturn = append(resourcesToReturn, newCachedResourceWithTTL(name, resource, version))
				returnedResources[name] = version
			}
		} else {
			resourcesToReturn = make([]*cachedResource, 0, len(subscribedResources))
			returnedResources = make(map[string]string, len(subscribedResources))
		}
		for name := range subscribedResources {
			resource, ok := resources[name]
			if !ok {
				continue
			}
			resourcesToReturn = append(resourcesToReturn, newCachedResourceWithTTL(name, resource, version))
			returnedResources[name] = version
		}
	} else {
		// Same version and not full state, only return newly subscribed resources.
		returnedResources = maps.Clone(knownResources)
		for name := range changedResources {
			resource, ok := resources[name]
			if !ok {
				// Should never occur.
				continue
			}
			resourcesToReturn = append(resourcesToReturn, newCachedResourceWithTTL(name, resource, version))
			returnedResources[name] = version
		}
		for _, name := range deletedResources {
			// Cleanup resources no longer subscribed to make sure we resend them if re-subscribed later.
			delete(returnedResources, name)
		}
	}

	return &RawResponse{
		Request:           watch.Request,
		Version:           version,
		resources:         resourcesToReturn,
		returnedResources: returnedResources,
	}, false
}

func (cache *snapshotCache) respond(ctx context.Context, watch ResponseWatch, response *RawResponse) error {
	request := watch.Request
	cache.log.Debugf("respond %s (requested %v) version %q with version %q and resources %v", request.GetTypeUrl(), request.GetResourceNames(), request.GetVersionInfo(), response.Version, slices.Collect(maps.Keys(response.GetReturnedResources())))
	response.Ctx = ctx

	select {
	case watch.Response <- response:
		return nil
	case <-watch.done:
		return nil
	case <-ctx.Done():
		return context.Canceled
	}
}

// CreateDeltaWatch returns a watch for a delta xDS request which implements the Simple SnapshotCache.
func (cache *snapshotCache) CreateDeltaWatch(request *DeltaRequest, sub Subscription, value chan DeltaResponse) (func(), error) {
	cache.mu.Lock()
	cancel, watch, resp, err := cache.prepareDeltaWatch(request, sub, value)
	cache.mu.Unlock()
	if err != nil {
		return cancel, err
	}
	if resp != nil {
		if err := cache.respondDelta(context.Background(), watch, resp); err != nil {
			return func() {}, fmt.Errorf("responding: %w", err)
		}
	}
	return cancel, nil
}

// prepareDeltaWatch requires cache.mu to be held for writing.
func (cache *snapshotCache) prepareDeltaWatch(request *DeltaRequest, sub Subscription, value chan DeltaResponse) (func(), DeltaResponseWatch, *RawDeltaResponse, error) {
	nodeID := cache.hash.ID(request.GetNode())
	t := request.GetTypeUrl()

	info, ok := cache.status[nodeID]
	if !ok {
		info = newStatusInfo(request.GetNode())
		cache.status[nodeID] = info
	}

	// update last watch request time
	info.setLastDeltaWatchRequestTime(time.Now())

	watch := DeltaResponseWatch{Request: request, Response: value, subscription: sub}

	// find the current cache snapshot for the provided node
	snapshot, exists := cache.snapshots[nodeID]
	if exists {
		err := snapshot.ConstructVersionMap()
		if err != nil {
			return func() {}, watch, nil, fmt.Errorf("computing version map: %w", err)
		}

		resp := createDeltaResponse(snapshot, watch, sub.IsWildcard() && request.ResponseNonce == "")
		if resp != nil {
			return func() {}, watch, resp, nil
		}

		// We did not reply, fallthrough to watch tracking
	}

	// There are two different cases that leads to a delayed watch trigger:
	// - no snapshot exists for the requested nodeID
	// - we attempted to issue a response, but the caller is already up to date
	watchID := cache.nextDeltaWatchID()
	if exists {
		cache.log.Infof("open delta watch ID:%d for %s Resources:%v from nodeID: %q,  version %q", watchID, t, sub.SubscribedResources(), nodeID, snapshot.GetVersion(t))
	} else {
		cache.log.Infof("open delta watch ID:%d for %s Resources:%v from nodeID: %q", watchID, t, sub.SubscribedResources(), nodeID)
	}

	done := make(chan struct{})
	watch.done = done
	info.setDeltaResponseWatch(watchID, watch)
	return cache.cancelDeltaWatch(nodeID, watchID, done), watch, nil, nil
}

func createDeltaResponse(snapshot ResourceSnapshot, watch DeltaResponseWatch, replyIfEmpty bool) *RawDeltaResponse {
	typeURL := watch.Request.TypeUrl

	version := snapshot.GetVersion(typeURL)
	resources := snapshot.GetResourcesAndTTL(typeURL)
	versionMap := snapshot.GetVersionMap(typeURL)
	subscribed := watch.subscription.SubscribedResources()

	var resourcesToReturn []*cachedResource
	var deletedResources []string
	returnedResources := maps.Clone(watch.subscription.ReturnedResources())

	if watch.subscription.IsWildcard() {
		resourcesToReturn = make([]*cachedResource, 0, len(resources)+len(subscribed))
	} else {
		resourcesToReturn = make([]*cachedResource, 0, len(subscribed))
	}

	// Check if a resource was not previously returned or has changed version.
	addIfChanged := func(name string, res types.ResourceWithTTL) {
		resVersion := versionMap[name] // Version in snapshot
		knownVersion, known := returnedResources[name]
		if known && knownVersion == resVersion {
			return
		}
		resourcesToReturn = append(resourcesToReturn, newCachedResourceWithTTL(name, res, version))
		returnedResources[name] = resVersion
	}

	if watch.subscription.IsWildcard() {
		for name, res := range resources {
			addIfChanged(name, res)
		}
	}

	for name := range subscribed {
		res, exist := resources[name]
		if !exist {
			continue
		}
		addIfChanged(name, res)
	}

	for name := range returnedResources {
		_, exist := resources[name]
		if !exist {
			deletedResources = append(deletedResources, name)
			delete(returnedResources, name)
			continue
		}

		if watch.subscription.IsWildcard() {
			continue
		}
		if _, watched := subscribed[name]; !watched {
			// Resource is no longer watched.
			deletedResources = append(deletedResources, name)
			delete(returnedResources, name)
		}
	}

	if len(resourcesToReturn) == 0 && len(deletedResources) == 0 && !replyIfEmpty {
		// Nothing's changed
		return nil
	}

	return &RawDeltaResponse{
		DeltaRequest:      watch.Request,
		SystemVersionInfo: version,
		resources:         resourcesToReturn,
		removedResources:  deletedResources,
		returnedResources: returnedResources,
	}
}

// Respond to a delta watch with the provided snapshot value. If the response is nil, there has been no state change.
func (cache *snapshotCache) respondDelta(ctx context.Context, watch DeltaResponseWatch, resp *RawDeltaResponse) error {
	cache.log.Debugf("node: %s, sending delta response for typeURL %s with resources: %v removed resources: %v with wildcard: %t",
		watch.Request.GetNode().GetId(), watch.Request.GetTypeUrl(), getCachedResourceNames(resp.resources), resp.removedResources, watch.subscription.IsWildcard())
	resp.Ctx = ctx
	select {
	case watch.Response <- resp:
		return nil
	case <-watch.done:
		return nil
	case <-ctx.Done():
		return context.Canceled
	}
}

func (cache *snapshotCache) nextDeltaWatchID() int64 {
	return cache.deltaWatchCount.Add(1)
}

// cancellation function for cleaning stale delta watches.
func (cache *snapshotCache) cancelDeltaWatch(nodeID string, watchID int64, done chan struct{}) func() {
	var once sync.Once
	return func() {
		once.Do(func() { close(done) })
		cache.mu.RLock()
		var idleCleared bool
		if info, ok := cache.status[nodeID]; ok {
			info.mu.Lock()
			delete(info.deltaWatches, watchID)
			idleCleared = info.snapshotCleared && len(info.watches) == 0 && len(info.deltaWatches) == 0
			info.mu.Unlock()
		}
		cache.mu.RUnlock()
		if idleCleared {
			cache.dropClearedStatus(nodeID)
		}
	}
}

// Fetch implements the cache fetch function.
// Fetch is called on multiple streams, so responding to individual names with the same version works.
func (cache *snapshotCache) Fetch(ctx context.Context, request *Request) (Response, error) {
	nodeID := cache.hash.ID(request.GetNode())

	cache.mu.RLock()
	defer cache.mu.RUnlock()

	snapshot, exists := cache.snapshots[nodeID]
	if !exists {
		return nil, fmt.Errorf("missing snapshot for %q", nodeID)
	}

	// Respond only if the request version is distinct from the current snapshot state.
	// It might be beneficial to hold the request since Envoy will re-attempt the refresh.
	version := snapshot.GetVersion(request.GetTypeUrl())
	if request.GetVersionInfo() == version {
		cache.log.Warnf("skip fetch: version up to date")
		return nil, &types.SkipFetchError{}
	}

	resources := snapshot.GetResourcesAndTTL(request.GetTypeUrl())
	var resourcesToReturn []*cachedResource
	if len(request.ResourceNames) == 0 {
		resourcesToReturn = make([]*cachedResource, 0, len(resources))
		for name, res := range resources {
			resourcesToReturn = append(resourcesToReturn, newCachedResourceWithTTL(name, res, version))
		}
	} else {
		for _, name := range request.ResourceNames {
			res, ok := resources[name]
			if !ok {
				continue
			}
			resourcesToReturn = append(resourcesToReturn, newCachedResourceWithTTL(name, res, version))
		}
	}

	return &RawResponse{
		Request:   request,
		Version:   version,
		resources: resourcesToReturn,
		Ctx:       ctx,
	}, nil
}

// GetStatusInfo retrieves the status info for the node.
func (cache *snapshotCache) GetStatusInfo(node string) StatusInfo {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	info, exists := cache.status[node]
	if !exists {
		cache.log.Warnf("node does not exist")
		return nil
	}

	return info
}

// GetStatusKeys retrieves all node IDs in the status map.
func (cache *snapshotCache) GetStatusKeys() []string {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	out := make([]string, 0, len(cache.status))
	for id := range cache.status {
		out = append(out, id)
	}

	return out
}
