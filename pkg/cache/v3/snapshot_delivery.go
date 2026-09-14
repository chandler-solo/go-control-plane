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

import "context"

// nodeResponseLock serializes publications for one node without blocking other
// nodes. References include waiters; entries disappear when their last user exits.
type nodeResponseLock struct {
	ready chan struct{}
	refs  int
}

func (cache *snapshotCache) lockResponses(ctx context.Context, node string) (func(), error) {
	cache.mu.Lock()
	if cache.responseLocks == nil {
		cache.responseLocks = make(map[string]*nodeResponseLock)
	}
	gate := cache.responseLocks[node]
	if gate == nil {
		gate = &nodeResponseLock{ready: make(chan struct{}, 1)}
		gate.ready <- struct{}{}
		cache.responseLocks[node] = gate
	}
	gate.refs++
	cache.mu.Unlock()
	releaseReference := func() {
		cache.mu.Lock()
		gate.refs--
		if gate.refs == 0 {
			delete(cache.responseLocks, node)
		}
		cache.mu.Unlock()
	}
	// Preserve installation with an already-canceled context when no send is
	// pending, while allowing a caller waiting behind a blocked send to cancel.
	select {
	case <-gate.ready:
	default:
		select {
		case <-gate.ready:
		case <-ctx.Done():
			releaseReference()
			return nil, ctx.Err()
		}
	}
	return func() { gate.ready <- struct{}{}; releaseReference() }, nil
}

type pendingSotwResponse struct {
	id       int64
	watch    ResponseWatch
	response *RawResponse
}

type pendingDeltaResponse struct {
	id       int64
	watch    DeltaResponseWatch
	response *RawDeltaResponse
}

// sendResponses delivers a batch after releasing the cache and status locks.
// Unsent watches are restored on failure unless their caller canceled them.
func (cache *snapshotCache) sendResponses(ctx context.Context, node string, info *statusInfo, sotw []pendingSotwResponse, delta []pendingDeltaResponse) error {
	total := len(sotw) + len(delta)
	if total == 0 {
		return nil
	}
	defer func() {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		info.mu.Lock()
		defer info.mu.Unlock()
		info.responsesInFlight -= total
		for _, pending := range sotw {
			select {
			case <-pending.watch.done:
			default:
				info.watches[pending.id] = pending.watch
			}
		}
		for _, pending := range delta {
			select {
			case <-pending.watch.done:
			default:
				info.deltaWatches[pending.id] = pending.watch
			}
		}
		cache.removeClearedStatus(node, info)
	}()
	for len(sotw) > 0 {
		pending := sotw[0]
		if err := cache.respond(ctx, pending.watch, pending.response); err != nil {
			return err
		}
		sotw = sotw[1:]
	}
	for len(delta) > 0 {
		pending := delta[0]
		if err := cache.respondDelta(ctx, pending.watch, pending.response); err != nil {
			return err
		}
		delta = delta[1:]
	}
	return nil
}
