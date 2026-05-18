# ANN Index Bundle Specification

**Version:** 1.0
**Status:** Normative
**Last Updated:** 2026-05-14

## 1. Overview

This document defines the on-disk format produced by spark-ann's offline
builder and consumed by any compliant runtime (offline batch search,
online HTTP server, future C++/Rust/GPU server). It is the public
contract between the builder side and the reader side.

The reference reader implementation lives in `com.wayblink.ann.bundle`
(see `index-bundle/src/main/scala/com/wayblink/ann/bundle/BundleReader.scala`).
Any conforming reader written in another language MUST produce identical
search results given identical bundles and queries.

### Normative language

The keywords MUST, MUST NOT, SHOULD, SHOULD NOT, MAY, REQUIRED, and
OPTIONAL in this document follow [RFC 2119]. Sections explicitly
marked "informative" are non-normative and exist only to clarify
intent.

[RFC 2119]: https://www.rfc-editor.org/rfc/rfc2119

## 2. Directory layout (normative)

A bundle is a single directory on shared storage (HDFS, S3, GCS, OSS,
or a local filesystem) containing the files listed below. Reader
behaviour is undefined when files are missing, mis-named, or carry
unsupported encodings.

```
<bundle-root>/
├── ann_index.json                   REQUIRED
├── local/                           REQUIRED, non-empty
│   ├── <indexId>.hnsw               REQUIRED, ≥1 entry
│   └── <indexId>.hnsw.meta          REQUIRED, hnswlib sidecar
├── global/                          REQUIRED iff numLocalIndexes > 1
│   ├── global_routing.hnsw          REQUIRED if directory is present
│   ├── global_routing.hnsw.meta     REQUIRED if global_routing.hnsw is present
│   └── boundary_mapping.json        REQUIRED if global_routing.hnsw is present
└── data/                            OPTIONAL (raw parquet, not used by readers)
```

- All file paths inside the bundle MUST be relative to `<bundle-root>`.
- All JSON files MUST be UTF-8 encoded.
- Filenames are case-sensitive on all platforms.
- The `data/` directory carries the raw Parquet inputs from the build.
  Readers MUST NOT depend on its contents; it is preserved only so a
  bundle can be rebuilt without re-uploading data. A bundle that
  omits `data/` is still conforming.

## 3. `ann_index.json` schema (normative)

`ann_index.json` MUST be a JSON document matching the envelope shape:

```json
{
  "version": 2,
  "type": "ANNIndexMetadata",
  "payload": { ... see below ... }
}
```

- `version` (int, REQUIRED): JSON envelope schema version. v1.0 of this
  spec corresponds to `version = 2`. Readers MUST reject documents
  whose `version` is greater than the latest version the reader
  recognises. Lower `version` values MUST be accepted as long as the
  required-field set is satisfied.
- `type` (string, REQUIRED): MUST equal `"ANNIndexMetadata"`. Readers
  MUST reject other values.
- `payload` (object, REQUIRED): The `ANNIndexMetadata` value defined
  below.

### 3.1 `ANNIndexMetadata` payload

| Field             | Type                          | Required | Notes |
|-------------------|-------------------------------|----------|-------|
| `indexPath`       | string                        | yes      | Bundle root path. Informational; readers SHOULD prefer the actual path they were given. |
| `localIndexes`    | array of `LocalIndexMetadata` | yes      | Length MUST be ≥ 1. |
| `globalIndexPath` | string or null                | yes      | When present, path to the global routing `.hnsw` file. When `null`, bundle has no routing index and readers MUST search all local indexes for every query. |
| `config`          | `ANNIndexConfig` (object)     | yes      | See §3.3. |
| `statistics`      | `ANNIndexStatistics` (object) | yes      | See §3.4. |
| `createdAt`       | int (epoch ms)                | yes      | Build timestamp. Informational. |

### 3.2 `LocalIndexMetadata`

| Field          | Type                       | Required | Notes |
|----------------|----------------------------|----------|-------|
| `indexId`      | string                     | yes      | Unique within a bundle. MUST match the basename (without `.hnsw`) of the file at `indexPath`. Used as the partition key for routing. |
| `dataFiles`    | array of `DataFileEntry`   | yes      | Source parquet files. Informational at read time. |
| `indexPath`    | string                     | yes      | Absolute path to the `.hnsw` file. |
| `totalVectors` | long                       | yes      | Number of vectors in this local index. MUST equal the HNSW file's record count. |
| `dimension`    | int                        | yes      | Vector dimensionality. MUST match every other local index in the same bundle. |

#### `DataFileEntry`

| Field          | Type   | Required | Notes |
|----------------|--------|----------|-------|
| `filePath`     | string | yes      | Source parquet path. Informational. |
| `numVectors`   | long   | yes      | Rows contributed by this file. |
| `vectorOffset` | long   | yes      | HNSW internal id of the first vector from this file (sequential-id mode). Informational when `pk` is set. |

### 3.3 `ANNIndexConfig`

| Field                   | Type                | Required | Default      | Notes |
|-------------------------|---------------------|----------|--------------|-------|
| `M`                     | int                 | yes      | 16           | HNSW M parameter. |
| `efConstruction`        | int                 | yes      | 200          | HNSW build-time accuracy knob. |
| `groupingStrategy`      | string              | yes      | `"SingleFile"` | One of `"SingleFile"` or `"MergeSmall"`. |
| `targetVectorsPerIndex` | long                | yes      | 500000       | Used only by `MergeSmall`. |
| `boundaryNodesPerIndex` | int                 | yes      | 50           | Routing samples per local index. |
| `distanceType`          | string              | yes      | `"euclidean"`| One of `"euclidean"` or `"cosine"`. Future versions MAY add `"inner_product"`, `"hamming"`. MUST be consistent across local + global indexes within a bundle. |
| `pk`                    | string or null      | yes      | null         | Optional primary-key column name. When non-null, the HNSW internal ids in every `.hnsw` file ARE the values of this column (see §6). When null, HNSW internal ids are per-local-index sequential offsets starting at 0. |
| `algorithm`             | string              | OPTIONAL | `"hnsw"`     | Index algorithm. v1.0 readers MUST accept the value `"hnsw"`; they SHOULD reject unknown values but MAY treat unset as `"hnsw"` for forward compat with this version. |

### 3.4 `ANNIndexStatistics`

| Field              | Type | Required | Notes |
|--------------------|------|----------|-------|
| `totalVectors`     | long | yes      | Sum of `localIndexes[*].totalVectors`. |
| `totalFiles`       | int  | yes      | Total parquet files across all local indexes. Informational. |
| `numLocalIndexes`  | int  | yes      | `localIndexes.length`. |
| `dimension`        | int  | yes      | Common dimension. |
| `buildTimeMs`      | long | yes      | Wall-clock build duration. Informational. |

## 4. `boundary_mapping.json` schema (normative)

REQUIRED when `globalIndexPath` is non-null. The envelope shape:

```json
{
  "version": 2,
  "type": "BoundaryNodeMapping",
  "payload": [
    { "globalId": 0, "indexId": "idx_a", "localId": 17 },
    { "globalId": 1, "indexId": "idx_b", "localId": 42 },
    ...
  ]
}
```

- `payload` MUST be an array sorted in ascending order of `globalId`.
- For every entry: `globalId` MUST equal the entry's array index AND
  MUST equal the HNSW internal id that the global routing index will
  return for the corresponding boundary node.
- `indexId` MUST refer to an entry in `ann_index.json` ▶
  `localIndexes[*].indexId`.
- `localId` is informational at routing time; readers MUST NOT depend
  on it for correctness of search results — the local search step
  rediscovers neighbours itself.

Readers MUST load the payload into an `Array[String]` indexed by
`globalId` so routing-result translation is O(1) per hit.

## 5. `.hnsw` file format (informative + warning)

The `.hnsw` files in a bundle use the binary format produced by
[`com.github.jelmerk:hnswlib-core`][jelmerk-hnswlib] version 1.1.0.
Each `.hnsw` file is a directory containing a hnswlib-managed binary
plus an `.hnsw.meta` sidecar — a UTF-8 JSON envelope of the form
`{"version":1,"type":"HNSWLibIndexMetadata","payload":{"dimension":N,
"distanceType":"euclidean|cosine","vectorCount":N}}`. Readers MUST
reject envelopes whose `version` exceeds the supported version and
MUST reject mismatched `type`. (Earlier drafts of this spec used a
Java `ObjectStream` payload; that format is no longer accepted.)

> ⚠️ **WARNING — Cross-language compatibility**
>
> The jelmerk-hnswlib format is **not binary-compatible** with the
> original C++ [`nmslib/hnswlib`][nmslib-hnswlib]. A future Rust / Go /
> C++ implementation of this spec CANNOT consume `.hnsw` files
> produced by this version directly; it MUST either embed a JVM via
> JNI or wait for a future spec version to define a neutral binary
> envelope. This limitation is recorded here, not hidden — bundle
> consumers should plan accordingly.

A future v2.0 of this spec MAY add a neutral envelope (e.g. a magic
prefix + protobuf/FlatBuffers metadata + a vendor-id field selecting
the binary payload format). Until then, JVM consumers using the
reference `BundleReader` are the only conforming readers.

[jelmerk-hnswlib]: https://github.com/jelmerk/hnswlib
[nmslib-hnswlib]: https://github.com/nmslib/hnswlib

## 6. HNSW internal id semantics (normative)

The `id` field returned by an HNSW search points back to the original
vector via one of two conventions, fixed at build time:

- **Sequential mode** (`config.pk` is null): IDs are per-local-index
  `Long` values starting at 0. Reader code MUST treat each local
  index's id space as independent — id `42` in `idx_a` and id `42`
  in `idx_b` refer to different vectors. The `(indexId, id)` pair is
  the only globally unique identifier.

- **PK passthrough mode** (`config.pk = "<column>"`): The Parquet
  column named `<column>` (which MUST be `INT32` or `INT64`) is
  loaded into the HNSW as the internal id. Search results carry the
  user's pk value directly — `(indexId, id)` is still globally unique
  in principle, but `id` alone is the business-meaningful key the
  user provided.
  - The bundle does NOT carry a separate id-mapping table. A future
    spec version MAY introduce one for String / UUID pks; until then,
    String pks MUST be rejected at build time.
  - User pk values MUST be unique within each local index; HNSW
    rejects duplicate ids inside a single index. Cross-local-index
    duplicates are allowed but expose the same `(indexId, id)`-as-
    primary-key behaviour as Sequential mode.

## 7. Algorithm field (normative)

`config.algorithm` records the index family. v1.0 defines exactly one
value:

- `"hnsw"` — Hierarchical Navigable Small World, the family used in
  every section above.

Future versions MAY add additional values (e.g. `"ivf"`, `"hnsw-ivf"`,
`"scann"`). Each new value MUST also specify how the on-disk layout
of the corresponding local index files differs from `.hnsw`. The
algorithm field is the single point of dispatch — readers MUST switch
on it before assuming any binary layout for `local/*.hnsw`.

## 8. Versioning rules (normative)

- A change to the on-disk layout that introduces a new required field
  is a **major** change and MUST increment the envelope `version`.
- A change that adds a new OPTIONAL field with a documented default is
  a **minor** change and MUST NOT increment `version`. Readers MUST
  ignore unknown fields they don't recognise.
- A change that removes a field is a major change.
- This spec is currently v1.0 / envelope `version = 2`. (The envelope
  version is offset because envelope shape `version = 1` predates
  this document.)

Readers MUST check the envelope `version` before parsing the payload
and MUST reject documents whose version is greater than the maximum
the reader recognises. The reference reader's max is
`MetadataJson.CurrentVersion`.

## 9. Distance metrics (normative)

v1.0 defines two values for `config.distanceType`:

- `"euclidean"` — L2 distance. Lower = more similar.
- `"cosine"` — Cosine distance = 1 - cosine_similarity. Lower = more
  similar. Best for L2-normalized embeddings.

`distanceType` MUST be consistent across all local indexes and the
global routing index within a bundle. A reader observing inconsistent
distance types MUST refuse to load the bundle.

Future spec versions MAY add `"inner_product"`, `"hamming"`,
`"jaccard"`. Each addition MUST clarify whether lower or higher
distance means more similar.

## 10. Conformance (normative)

A conforming reader MUST:

1. Reject a path that is not a directory or lacks `ann_index.json`.
2. Reject envelopes whose `version` is greater than the maximum the
   reader recognises, or whose `type` does not match the expected
   value.
3. Reject `config.algorithm` values it does not implement (when it
   chooses to enforce; v1.0 readers MAY treat unset as `"hnsw"` for
   forward compat with bundles written before §7 existed).
4. For a query against a bundle with `globalIndexPath != null`, route
   via the global routing index per §4 before searching local indexes.
   Without a global routing index, the reader MUST search every local
   index.
5. Return search results whose `id` field obeys §6.

A conforming reader SHOULD:

- Cache loaded `HNSWLibIndex` instances per process (online servers)
  or per executor (Spark workers) rather than re-loading per query.
- Validate that every local index's `dimension` matches a query's
  vector length before searching.
- Surface envelope, type, version, and algorithm rejection errors as
  typed values (e.g. `BundleError`) rather than free-form strings.

The reference reader passes all the tests in
`index-bundle/src/test/scala/com/wayblink/ann/bundle/`. Future
implementations SHOULD provide an equivalent test suite.

## 11. Change history

| Version | Date       | Notes |
|---------|------------|-------|
| 1.0     | 2026-05-14 | Initial spec, formalising the layout shipped in spark-ann commits 01391c2 → 3ac280d. |
