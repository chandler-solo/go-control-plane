# SnapshotCache

[SnapshotCache](https://github.com/envoyproxy/go-control-plane/blob/main/pkg/cache/v3/simple.go#L40) is a snapshot-based caching mechanism that maintains a versioned config snapshot per node. See the original README scope section for more detailed explanations on the individual cache systems.

> *NOTE*: SnapshotCache can operate as a REST or bi-di streaming xDS server

## Create a Snapshot Cache

To create a snapshot cache, we simply call the provided constructor:
```go
// Create a cache
cache := cache.NewSnapshotCache(false, cache.IDHash{}, nil)
```
This `cache` object holds a fully compliant [SnapshotCache](https://github.com/envoyproxy/go-control-plane/blob/main/pkg/cache/v3/simple.go#L40) with the necessary methods to manage a configuration cache lifecycle.

> *NOTE*: The cache needs a high access level inside the management server as it's the source of truth for xDS resources.

## Snapshots

In ADS mode, a named request is retained as an open watch when the snapshot
contains resources outside the subscription and cannot yet be sent. A later
snapshot aligned with the subscription answers that watch without another
client request. These retained requests are included in `GetNumWatches` and
can be removed using the cancellation function returned by `CreateWatch`.

Snapshots are groupings of resources at a given point in time for a node cluster. In other words Envoy's and consuming xDS clients registering as node `abc` all share a snapshot of config. This snapshot is the singular source of truth in the cache that represents config for any of those consumers.

> *NOTE*: Snapshots can be partial, e.g., only including RDS or EDS resources. 

```go
snap, err := cache.NewSnapshot("v0", map[resource.Type][]types.Resource{
    resource.EndpointType:        endpoints,
    resource.ClusterType:         clusters,
    resource.RouteType:           routes,
    resource.ScopedRouteType:     scopedRoutes,
    resource.ListenerType:        listeners,
    resource.RuntimeType:         runtimes,
    resource.SecretType:          secrets,
    resource.ExtensionConfigType: extensions,
})
```

For a more in-depth example of how to genereate a snapshot, explore our example found [here](https://github.com/envoyproxy/go-control-plane/blob/main/internal/example/resource.go#L168).

We recommend verifying that your new `snapshot` is consistent within itself meaning that the dependent resources are exactly listed in the snapshot:

```go
if err := snapshot.Consistent(); err != nil {
   l.Errorf("snapshot inconsistency: %+v\n%+v", snapshot, err)
   os.Exit(1)
}
```

- all EDS resources are listed by name in CDS resources
- all SRDS/RDS resources are listed by name in LDS resources
- all RDS resources are listed by name in SRDS resources

> *NOTE*: Clusters and Listeners are requested without name references, so Envoy will accept the snapshot list of clusters as-is even if it does not match all references found in xDS.

Setting a snapshot is as simple as:
```go
// Add the snapshot to the cache
if err := cache.SetSnapshot("envoy-node-id", snapshot); err != nil {
    l.Errorf("snapshot error %q for %+v", err, snapshot)
    os.Exit(1)
}
```

This will trigger all open watches internal to the caching [config watchers](https://github.com/envoyproxy/go-control-plane/blob/main/pkg/cache/v3/cache.go#L45) and anything listening for changes will received updates and responses from the new snapshot.

*Note*: that a node ID must be provided along with the snapshot object. Internally a mapping of the two is kept so each node can receive the latest version of its configuration.

## Clearing snapshots

`ClearSnapshot(node)` removes the cached snapshot. Open SotW and delta watches
remain registered so the next `SetSnapshot` for that node can answer them.
Clearing does not send a response or close response channels. Status remains
available while watches are open and is removed when the last watch is canceled.
Publishing another snapshot keeps the node's status available as usual.

To discard both the snapshot and node status immediately, cancel the node's
streams and their watches before calling `ClearSnapshot`.

This changes what callers observe: `GetStatusInfo(node)` returns a non-nil
status for a cleared node while any of its watches are open. Code that used a
nil status after `ClearSnapshot` as the signal that a node is gone should check
`GetSnapshot` for the absent snapshot instead, or cancel the streams first.
Cancelling a watch takes only the read lock unless it is the last watch of a
cleared node, so the retained status does not slow down watch churn.

## Subscription-filtered ADS responses

The default ADS policy waits until every resource in a snapshot type is named
by the client. When snapshots include resources that a client will never
request, its named responses can remain blocked across revisions. To answer
with just the client's subscribed resources, opt in when constructing the cache:

```go
c := cache.NewSnapshotCacheWithOptions(cache.IDHash{}, nil,
    cache.WithADS(), cache.WithSubscriptionFilteredResponses())
```

The option affects immediate and parked named SotW ADS watches. Wildcard
responses still include all resources. Snapshot maps are not modified, and
equal-version requests still return newly subscribed resources. Empty
subscriptions receive no response and register no watch. Delta watches and
REST fetches are unchanged.

`NewSnapshotCache(ads, hash, logger)` keeps its signature and default policy.
`NewSnapshotCacheWithOptions(hash, logger)` defaults to non-ADS mode; use
`WithADS()` alone to select the legacy ADS policy. Filtering is opt-in because
some consumers may use the legacy delay to coordinate resource warming.

What the option gives up is that coordination. The legacy policy answers a
named type only once the client has named every resource of that type in the
snapshot, which makes Envoy wait for its CDS or LDS to name every cluster or
route before any endpoints or routes arrive. With filtering, a client that
names a resource absent from the snapshot is answered without it and keeps
waiting for that name; a resource it has not named is never sent to it; and a
newly added cluster or route reaches the client when it re-requests with the
new name after its CDS or LDS changes, which Envoy does on every such change.
When the legacy policy holds a response, the cache logs it at debug level with
the watch, type and names, so a withheld named type can be found in logs.
