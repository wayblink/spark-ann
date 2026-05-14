package com.wayblink.ann.bundle

import org.json4s._
import org.json4s.jackson.Serialization

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

/**
 * JSON (de)serialization for ANN bundle metadata. Uses json4s-jackson,
 * already a transitive dependency of Spark 3.5 and pulled in
 * standalone for non-Spark consumers.
 *
 * Writes a versioned envelope per file so readers can reject unknown
 * majors. Replaces the prior Java ObjectStream serialization, which
 * was fragile across field additions and opaque to tooling.
 */
object MetadataJson {

  val CurrentVersion: Int = 2
  val MetadataType: String = "ANNIndexMetadata"
  val BoundaryMappingType: String = "BoundaryNodeMapping"

  // Custom serializer for the GroupingStrategy sealed trait. NoTypeHints
  // alone can't round-trip case objects, so the on-disk form is the
  // human-readable class name string. Adding a third case object means
  // extending this match — the compiler will flag any missing branch
  // via the exhaustive deserialiser case.
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
