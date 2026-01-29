# Spark-ANN TODO

## High Priority

### 1. Index building runs on driver, not distributed
- `LocalIndexBuilder.buildFromFileGroups` iterates sequentially on the driver with `.map`
- `collectBoundaryNodes` in `ANNIndexBuilder` calls `spark.read.parquet(...).collect()` inside a loop, pulling all vector data to the driver
- `batchSearch` in `ANNSearcher` calls `queries.collect()`, again pulling everything to the driver
- The system uses Spark only as a Parquet reader, not for distributed computation
- For large datasets, this will OOM the driver
- **Action:** Use `mapPartitions` or broadcast the HNSW config and build indexes in parallel on executors, then write each partition's index to HDFS/S3. Alternatively, if single-machine operation is intended, drop the Spark dependency and use a simpler Parquet reader.
- Files: `LocalIndexBuilder.scala:83`, `ANNIndexBuilder.scala:167-183`, `ANNSearcher.scala:117`

### 2. Thread-safety bugs in HNSWLibIndex
- `index.setEf(ef)` in `search` mutates shared state before `findNearest`. Concurrent calls with different `ef` values will race.
- `HNSWLibIndex.version` is a mutable `var` on the companion object, shared across all instances with no synchronization, and never incremented. Produces incorrect behavior when removing items from multiple indexes concurrently.
- **Action:** Pass `ef` per-query or synchronize access. Replace the shared `version` var with a per-instance AtomicInteger.
- Files: `HNSWLibIndex.scala:70`, `HNSWLibIndex.scala:132`, `HNSWLibIndex.scala:156`

### 3. API server cannot serve Spark-built indexes
- `api-server` depends only on `core`, not on `spark-integration`
- `IndexManager` manages simple in-memory indexes and cannot leverage multi-index routing, boundary-node search, or the distributed index structure
- No way to serve indexes built by the Spark pipeline through the API server without manual re-loading; metadata formats are incompatible
- **Action:** Add an endpoint or service that can load and serve `ANNIndexMetadata`-based hierarchical indexes, or unify the metadata format between modules.

## Medium Priority

### 4. Java serialization for metadata is fragile
- `ObjectOutputStream`/`ObjectInputStream` used extensively for metadata
- Brittle across version changes (field additions/renames break deserialization)
- Known security risk (deserialization attacks)
- Not human-readable for debugging
- **Action:** Replace with JSON (json4s is already a dependency) or Protobuf.
- Files: `ANNIndexBuilder.scala:265`, `ANNSearcher.scala:233`, `HNSWLibIndex.scala:93-98`

### 5. No resource limits enforcement
- `max-loaded-indexes` config exists in `application.conf` but is never enforced in `IndexManager`
- No memory estimation before loading indexes — large index loads can OOM without warning
- No concurrent search limiting — under high load, all requests search simultaneously with no queueing or rejection
- **Action:** Enforce `max-loaded-indexes` in `IndexManager`. Add memory estimation or index size checks. Consider adding backpressure via Akka stream throttling or semaphores.
- Files: `IndexManager.scala`, `application.conf`

### 6. String-based error matching in routes
- `SearchRoutes` routes errors by checking `error.contains("not found")`, `error.contains("dimension")`, etc.
- If error message wording changes, HTTP status codes silently break
- **Action:** Replace with a sealed trait error hierarchy (e.g., `IndexNotFound`, `DimensionMismatch`, `SearchFailed`) and pattern match on types instead of strings.
- Files: `SearchRoutes.scala:42-48`, `SearchService.scala`

### 7. Index reloaded from disk on every search call
- `ANNDataFrameOps.annSearch` calls `ANNSearcher.load(...)` on every invocation, deserializing the entire index from disk
- No caching — repeated queries against the same index pay the full deserialization cost each time
- **Action:** Add an index cache (e.g., LRU cache keyed by `indexPath`) in `ANNSearcher` or at the API layer.
- Files: `ANNDataFrameAPI.scala:43`

### 8. Resource leaks in stream handling
- Multiple places open streams without fully safe try-with-resources patterns
- If `writeObject` throws, underlying file streams may not be properly flushed/closed
- **Action:** Wrap stream creation in `try`/`finally` that handles creation failure, or use a utility method. Scala 2.13+ has `Using`; for 2.12, a manual helper or loan pattern works.
- Files: `HNSWLibIndex.scala:93-98`, `ANNIndexBuilder.scala:283-289`, `ANNIndexBuilder.scala:265-274`, `ANNSearcher.scala:233-238`

### 9. Boundary node routing strategy is simplistic
- Global routing index selects boundary nodes via evenly-spaced sampling, ignoring actual data distribution
- `findIndexIdForGlobalId` does a linear scan through metadata for every routing result — O(n) per lookup on the search hot path
- **Action:** Use k-means centroids or medoids for boundary node selection. Replace linear scan with a precomputed lookup map or sorted array with binary search.
- Files: `ANNIndexBuilder.scala:190-213`, `ANNSearcher.scala:185-193`

## Low Priority

### 10. println instead of logging
- `ANNIndexBuilder`, `LocalIndexBuilder`, `FileDiscovery` use `println` for progress output
- Bypasses Logback (which is configured), makes log levels uncontrollable, and pollutes stdout in production
- **Action:** Replace `println` with SLF4J logger calls at appropriate levels (info/debug).

### 11. TOCTOU race in IndexManager
- `loadIndex` and `createIndex` check `containsKey` then later call `put` — another thread could insert the same key between the two calls
- **Action:** Use `putIfAbsent` atomically instead of check-then-put.
- Files: `IndexManager.scala:41-43`, `IndexManager.scala:78-79`

### 12. Implicit return usage
- Several methods use early `return` statements inside methods (e.g., `SearchService.search:73`, `SearchService.multiSearch:119,133,138,143`)
- `return` has surprising semantics inside lambdas/closures in Scala and is considered un-idiomatic
- **Action:** Refactor to `if`/`else` chains or `Either` chaining with `flatMap`.

### 13. validateSearchRequest is not a proper Akka directive
- `SearchRoutes.validateSearchRequest` calls `complete(...)` in branches but returns `Directive0`
- Works but is semantically confusing
- **Action:** Use `reject` with a custom rejection, or change the return type to `Route`.
- Files: `SearchRoutes.scala:192-204`

### 14. Code hygiene
- `HNSWConfig.M` uses uppercase naming, violating Scala conventions and potentially conflicting with pattern matching
- Unused import: `ObjectOutputStream` in `ANNSearcher.scala:9`
- Unused constant: `MetadataFileName` in `ANNSearcher.scala:220`
- Magic numbers: `nprobe * 2` (`ANNSearcher.scala:160`), `maxElements * 2` (`LocalIndexBuilder.scala:140`), `boundaryNodes.length * 2` (`ANNIndexBuilder.scala:229`) — unexplained multipliers
- `IndexManager.loadIndex` hardcodes `distanceType = "euclidean"` with a TODO comment (`IndexManager.scala:51`)
- **Action:** Fix naming, remove unused code, extract magic numbers into named constants, read distance type from metadata.
