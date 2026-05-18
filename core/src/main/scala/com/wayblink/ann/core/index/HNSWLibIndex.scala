package com.wayblink.ann.core.index

import com.github.jelmerk.knn.{DistanceFunction, DistanceFunctions}
import com.github.jelmerk.knn.hnsw.HnswIndex
import org.json4s._
import org.json4s.jackson.Serialization

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters._

/**
 * HNSW Index implementation backed by hnswlib-core library.
 * Provides efficient approximate nearest neighbor search using the HNSW algorithm.
 *
 * @param dim The dimensionality of vectors in this index
 * @param config HNSW configuration parameters
 * @param distanceType Type of distance metric ("euclidean" or "cosine")
 */
class HNSWLibIndex private (
  val dim: Int,
  config: HNSWConfig,
  distanceType: String
) extends HNSWIndex with Serializable {

  @transient private var index: HnswIndex[Long, Array[Float], VectorItem, java.lang.Float] =
    createIndex(config)

  // Per-instance monotonic counter passed to hnswlib's optimistic-concurrency
  // remove API. Was previously a shared companion-object var, which corrupted
  // tombstone tracking across concurrent indices.
  @transient private val removeVersion: AtomicLong = new AtomicLong(0L)

  private def createIndex(cfg: HNSWConfig): HnswIndex[Long, Array[Float], VectorItem, java.lang.Float] = {
    val distanceFunction: DistanceFunction[Array[Float], java.lang.Float] = distanceType match {
      case "cosine" => DistanceFunctions.FLOAT_COSINE_DISTANCE
      case _ => DistanceFunctions.FLOAT_EUCLIDEAN_DISTANCE
    }

    HnswIndex.newBuilder(dim, distanceFunction, cfg.maxElements)
      .withM(cfg.M)
      .withEfConstruction(cfg.efConstruction)
      .withEf(50) // Default search ef
      .withRemoveEnabled()
      .build()
  }

  override def dimension: Int = dim

  override def size: Int = if (index != null) index.size() else 0

  override def add(id: Long, vector: Array[Float]): Unit = {
    require(vector.length == dim,
      s"Vector dimension ${vector.length} doesn't match index dimension $dim")
    index.add(VectorItem(id, vector))
  }

  override def addAll(vectors: Seq[(Long, Array[Float])]): Unit = {
    val items = vectors.map { case (id, vector) =>
      require(vector.length == dim,
        s"Vector dimension ${vector.length} doesn't match index dimension $dim")
      VectorItem(id, vector)
    }
    // Use parallel addAll for better performance with large batches
    index.addAll(items.asJava)
  }

  override def search(query: Array[Float], k: Int, ef: Int = 50): Seq[SearchResult] = {
    require(query.length == dim,
      s"Query dimension ${query.length} doesn't match index dimension $dim")

    if (size == 0) {
      return Seq.empty
    }

    val actualK = math.min(k, size)

    // hnswlib's `ef` is mutable state on the index object; setting it from one
    // thread while another concurrent search reads it produces interleaved /
    // wrong-accuracy results. Serialize the setEf + findNearest pair per index.
    // Different indices remain independent (lock is on the underlying HnswIndex).
    val results = index.synchronized {
      index.setEf(ef)
      index.findNearest(query, actualK)
    }

    results.asScala.map { result =>
      SearchResult(
        id = result.item().id(),
        distance = result.distance()
      )
    }.toSeq
  }

  override def save(path: String): Unit = {
    val indexPath = Paths.get(path)
    Files.createDirectories(indexPath.getParent)

    // Save the index
    index.save(indexPath)

    // Save metadata as JSON sidecar (was Java ObjectStream — see BUNDLE_SPEC §5).
    val metadataPath = Paths.get(path + ".meta")
    val metadata = IndexMetadata(dim, distanceType, size)
    HNSWLibIndex.writeMetadata(metadata, metadataPath)
  }

  override def load(path: String): Unit = {
    val indexPath = Paths.get(path)
    val metadataPath = Paths.get(path + ".meta")

    val metadata = HNSWLibIndex.readMetadata(metadataPath)

    require(metadata.dimension == dim,
      s"Loaded index dimension ${metadata.dimension} doesn't match expected dimension $dim")

    index = HnswIndex.load(indexPath)
  }

  /**
   * Get all items in the index.
   * Useful for debugging and testing.
   */
  def getItems: Seq[(Long, Array[Float])] = {
    index.items().asScala.map(item => (item.id(), item.vector())).toSeq
  }

  /**
   * Remove an item from the index by ID.
   */
  def remove(id: Long): Boolean = {
    index.remove(id, removeVersion.incrementAndGet())
  }

  /**
   * Check if an item exists in the index.
   */
  def contains(id: Long): Boolean = {
    index.get(id).isPresent
  }
}

/**
 * Metadata for serialized indices. Persisted as a JSON envelope; the
 * `@SerialVersionUID` is retained only because the case class extends
 * `Serializable` for compatibility with Spark closure capture, not for
 * on-disk persistence.
 */
@SerialVersionUID(1L)
case class IndexMetadata(
  dimension: Int,
  distanceType: String,
  vectorCount: Int
) extends Serializable

object HNSWLibIndex {

  /** Version of the JSON sidecar envelope written by save() / accepted by load(). */
  val MetaSidecarVersion: Int = 1
  val MetaSidecarType: String = "HNSWLibIndexMetadata"

  private implicit val jsonFormats: Formats = Serialization.formats(NoTypeHints)

  private case class MetaEnvelope(version: Int, `type`: String, payload: IndexMetadata)

  private def writeMetadata(metadata: IndexMetadata, target: Path): Unit = {
    val envelope = MetaEnvelope(MetaSidecarVersion, MetaSidecarType, metadata)
    val json = Serialization.writePretty(envelope)
    Files.write(target, json.getBytes(StandardCharsets.UTF_8))
  }

  private def readMetadata(source: Path): IndexMetadata = {
    val text = new String(Files.readAllBytes(source), StandardCharsets.UTF_8)
    val envelope = Serialization.read[MetaEnvelope](text)
    if (envelope.version > MetaSidecarVersion) {
      throw new IllegalStateException(
        s"Metadata sidecar version ${envelope.version} is newer than supported $MetaSidecarVersion at $source"
      )
    }
    if (envelope.`type` != MetaSidecarType) {
      throw new IllegalStateException(
        s"Metadata sidecar type mismatch at $source: expected $MetaSidecarType, found ${envelope.`type`}"
      )
    }
    envelope.payload
  }

  /**
   * Create a new empty HNSWLibIndex with default configuration.
   */
  def apply(dimension: Int): HNSWLibIndex = {
    new HNSWLibIndex(dimension, HNSWConfig(), "euclidean")
  }

  /**
   * Create a new empty HNSWLibIndex with custom configuration.
   */
  def apply(dimension: Int, config: HNSWConfig): HNSWLibIndex = {
    new HNSWLibIndex(dimension, config, "euclidean")
  }

  /**
   * Create a new empty HNSWLibIndex with custom configuration and distance type.
   *
   * @param dimension Vector dimensionality
   * @param config HNSW configuration
   * @param distanceType "euclidean" or "cosine"
   */
  def apply(dimension: Int, config: HNSWConfig, distanceType: String): HNSWLibIndex = {
    new HNSWLibIndex(dimension, config, distanceType)
  }

  /**
   * Load an existing index from disk.
   */
  def load(path: String): HNSWLibIndex = {
    val metadataPath = Paths.get(path + ".meta")
    val metadata = readMetadata(metadataPath)

    val index = new HNSWLibIndex(metadata.dimension, HNSWConfig(), metadata.distanceType)
    index.index = HnswIndex.load(Paths.get(path))
    index
  }
}
