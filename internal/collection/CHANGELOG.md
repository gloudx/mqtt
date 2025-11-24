# Changelog - Collection Package

## [2.0.0] - 2025-11-23

### Major Changes - EventLog Integration

#### Breaking Changes

1. **Engine Constructor Updated**
   ```go
   // BEFORE
   NewEngine(db *badger.DB, nodeID string, registry *schema.Registry) *Engine
   
   // AFTER
   NewEngine(db *badger.DB, ownerDID *identity.DID, keyPair *identity.KeyPair, registry *schema.Registry) *Engine
   ```

2. **Event Return Type Changed**
   ```go
   // BEFORE
   Insert(ctx, doc) (*collection.Event, error)
   
   // AFTER
   Insert(ctx, doc) (*event.Event, error)
   ```
   
   The returned event now includes:
   - `EventTID` - Timestamp-based unique ID
   - `AuthorDID` - DID of the event author
   - `Collection` - Collection name
   - `EventType` - Operation type (create/update/delete)
   - `Parents` - Causal relationships
   - `Signature` - Event signature

#### New Features

1. **DID-based Identity**
   - Each node now has a decentralized identifier (DID)
   - Events are cryptographically signed by the node's private key
   - Full support for event verification

2. **Distributed Event Log**
   - Integration with `internal/eventlog` v2.1
   - DAG-based event structure with causal relationships
   - Support for conflict resolution (LastWriteWins, DIDPriority, Custom)
   - Independent event DAGs per collection

3. **Enhanced Event Structure**
   - Timestamp-based IDs (TID) for global ordering
   - Access control (Public/Private/Shared)
   - Optional encryption support
   - Parent event tracking for causality

4. **New Types**
   - `Operation` enum: create, update, delete
   - `Query` struct for collection queries
   - `QueryResult` struct for query responses

#### Modified APIs

- `Collection.Insert()` - Now returns `*event.Event`
- `Collection.Update()` - Now returns `*event.Event`
- `Collection.Delete()` - Now returns `*event.Event`
- `Collection.ApplyRemoteEvent()` - Now accepts `*event.Event`
- `Collection.applyEvent()` - Now works with `*event.Event`

#### Removed

- `generateEventID()` - No longer needed, TID used instead
- Old `Event` struct - Replaced with `event.Event`

#### Added Files

- `example_usage.go` - Complete usage example
- `README.md` - Comprehensive documentation

#### Migration Guide

See `README.md` section "Миграция с предыдущей версии"

### TODO

#### Critical
- [ ] Implement Storage adapter for EventLog with BadgerDB
- [ ] Add Synchronizer for distributed synchronization

#### Improvements
- [ ] Add ClockID configuration
- [ ] Implement garbage collection
- [ ] Add snapshot mechanism
- [ ] Improve error handling in NewCollection
- [ ] Add metrics and logging
- [ ] Write unit tests

### Dependencies Updated

- Added: `mqtt-http-tunnel/internal/eventlog`
- Added: `mqtt-http-tunnel/internal/event`
- Added: `mqtt-http-tunnel/internal/identity`
- Added: `mqtt-http-tunnel/internal/tid`

### Notes

This is a major breaking change that requires updating all code that uses the collection package. The new architecture provides:

1. Better support for distributed systems
2. Cryptographic verification of events
3. Proper conflict resolution
4. Causal consistency guarantees
5. DID-based identity system
