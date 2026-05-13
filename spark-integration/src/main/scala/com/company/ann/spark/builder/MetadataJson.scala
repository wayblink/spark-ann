package com.company.ann.spark.builder

import com.company.ann.spark.api.ANNIndexMetadata
import org.json4s._
import org.json4s.jackson.Serialization

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

/**
 * Envelope written to disk for all metadata files. Version guards future
 * schema migrations; readers reject unknown major versions.
 */
case class MetadataEnvelope[T](
  version: Int,
  `type`: String,
  payload: T
)

/**
 * Mapping entry persisted in boundary_mapping.json. Position in the JSON
 * array equals the global routing id, so lookups are O(1) once loaded.
 */
case class BoundaryMappingEntry(
  globalId: Int,
  indexId: String,
  localId: Long
)

/**
 * JSON (de)serialization for ANN metadata. Uses json4s-jackson, already a
 * transitive dependency of Spark 3.5. Replaces prior Java ObjectStream
 * serialization, which was fragile across field additions and opaque to
 * tooling.
 */
object MetadataJson {

  val CurrentVersion: Int = 2
  val MetadataType: String = "ANNIndexMetadata"
  val BoundaryMappingType: String = "BoundaryNodeMapping"

  private object GroupingStrategySerializer extends CustomSerializer[GroupingStrategy](_ => (
    { case JString(s) => s match {
        case "SingleFile" => SingleFile
        case "MergeSmall" => MergeSmall
        case other => throw new MappingException(s"Unknown GroupingStrategy: $other")
      }
    },
    { case s: GroupingStrategy => JString(s.toString) }
  ))

  implicit val formats: Formats =
    Serialization.formats(NoTypeHints) + GroupingStrategySerializer

  def writeMetadata(metadata: ANNIndexMetadata, targetPath: Path): Unit = {
    val envelope = MetadataEnvelope(CurrentVersion, MetadataType, metadata)
    writeEnvelope(envelope, targetPath)
  }

  def readMetadata(sourcePath: Path): ANNIndexMetadata = {
    val text = readText(sourcePath)
    validateEnvelope(text, MetadataType, sourcePath)
    Serialization.read[MetadataEnvelope[ANNIndexMetadata]](text).payload
  }

  def writeBoundaryMapping(entries: Array[BoundaryMappingEntry], targetPath: Path): Unit = {
    val envelope = MetadataEnvelope(CurrentVersion, BoundaryMappingType, entries.toList)
    writeEnvelope(envelope, targetPath)
  }

  def readBoundaryMapping(sourcePath: Path): Array[BoundaryMappingEntry] = {
    val text = readText(sourcePath)
    validateEnvelope(text, BoundaryMappingType, sourcePath)
    Serialization.read[MetadataEnvelope[List[BoundaryMappingEntry]]](text).payload.toArray
  }

  private def writeEnvelope[T <: AnyRef](envelope: MetadataEnvelope[T], targetPath: Path): Unit = {
    Files.createDirectories(targetPath.getParent)
    val json = Serialization.writePretty(envelope)
    Files.write(targetPath, json.getBytes(StandardCharsets.UTF_8))
  }

  private def readText(sourcePath: Path): String =
    new String(Files.readAllBytes(sourcePath), StandardCharsets.UTF_8)

  private def validateEnvelope(text: String, expectedType: String, sourcePath: Path): Unit = {
    val header = Serialization.read[EnvelopeHeader](text)
    if (header.version > CurrentVersion) {
      throw new IllegalStateException(
        s"Metadata version ${header.version} is newer than supported $CurrentVersion at $sourcePath"
      )
    }
    if (header.`type` != expectedType) {
      throw new IllegalStateException(
        s"Metadata type mismatch at $sourcePath: expected $expectedType, found ${header.`type`}"
      )
    }
  }

  private case class EnvelopeHeader(version: Int, `type`: String)
}
