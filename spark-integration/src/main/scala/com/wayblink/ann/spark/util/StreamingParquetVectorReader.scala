package com.wayblink.ann.spark.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.schema.MessageType

/**
 * Streaming, iterator-based parquet vector reader. Holds at most one row
 * group's records resident at a time (parquet's natural unit), yielding
 * vectors lazily so callers can pipe rows directly into HNSW without
 * materializing the whole file in heap.
 *
 * Use this instead of the eager `ParquetVectorReader.readVectors` in any
 * build path that must scale to multi-million-row file groups at high
 * dimension — at 500K × 768 × 4 = 1.5 GB, the eager reader would otherwise
 * dominate task heap.
 */
object StreamingParquetVectorReader {

  /**
   * Stream vectors from a Parquet file. The returned iterator owns the
   * underlying ParquetFileReader and closes it after exhaustion or upon
   * `close()`.
   *
   * @param filePath     File path (local, HDFS, or S3)
   * @param vectorColumn Vector column name (3-level LIST<float> in Spark's parquet output)
   * @param hadoopConf   Hadoop configuration for FileSystem access
   * @return Iterator yielding one Array[Float] per row
   */
  def streamVectors(
    filePath: String,
    vectorColumn: String,
    hadoopConf: Configuration
  ): Iterator[Array[Float]] = {
    val path = new Path(filePath)
    val inputFile = HadoopInputFile.fromPath(path, hadoopConf)
    val reader = ParquetFileReader.open(inputFile)
    new VectorIterator(reader, vectorColumn)
  }

  /**
   * Stream `(pk, vector)` tuples from a Parquet file. The pk column must
   * be parquet INT64 or INT32; anything else raises at open time so the
   * caller fails fast before reading any rows.
   *
   * Used when the user supplies a primary-key column so search results
   * carry their own pk back instead of HNSW's internal counter.
   */
  def streamRows(
    filePath: String,
    pkColumn: String,
    vectorColumn: String,
    hadoopConf: Configuration
  ): Iterator[(Long, Array[Float])] = {
    val path = new Path(filePath)
    val inputFile = HadoopInputFile.fromPath(path, hadoopConf)
    val reader = ParquetFileReader.open(inputFile)
    new PkVectorIterator(reader, pkColumn, vectorColumn)
  }

  private final class VectorIterator(
    reader: ParquetFileReader,
    vectorColumn: String
  ) extends Iterator[Array[Float]] {

    private val schema: MessageType = reader.getFooter.getFileMetaData.getSchema
    private val columnIOFactory = new ColumnIOFactory()
    // PyArrow / Python float lists round-trip as DOUBLE in parquet.
    // Detect once at schema time so the hot loop can branch without
    // inspecting primitive types per value.
    private val elementsAreDouble: Boolean = detectDoubleElement(schema, vectorColumn)

    private var pages: PageReadStore = _
    private var recordReader: org.apache.parquet.io.RecordReader[Group] = _
    private var rowsLeftInGroup: Long = 0L
    private var closed: Boolean = false

    advanceRowGroup()

    private def advanceRowGroup(): Unit = {
      pages = reader.readNextRowGroup()
      if (pages == null) {
        closeQuietly()
      } else {
        val columnIO = columnIOFactory.getColumnIO(schema)
        recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
        rowsLeftInGroup = pages.getRowCount
      }
    }

    override def hasNext: Boolean = {
      if (closed) return false
      if (rowsLeftInGroup <= 0) {
        advanceRowGroup()
        if (closed) return false
      }
      true
    }

    override def next(): Array[Float] = {
      if (!hasNext) throw new NoSuchElementException("No more vectors in parquet stream")
      val group = recordReader.read()
      rowsLeftInGroup -= 1
      readVector(group, vectorColumn, elementsAreDouble)
    }

    private def closeQuietly(): Unit = {
      if (!closed) {
        closed = true
        try reader.close()
        catch { case _: Throwable => () }
      }
    }
  }

  /**
   * Iterator yielding (pkLong, vector) tuples. Pk type is normalised to
   * Long up front; INT32 columns are widened.
   */
  private final class PkVectorIterator(
    reader: ParquetFileReader,
    pkColumn: String,
    vectorColumn: String
  ) extends Iterator[(Long, Array[Float])] {

    private val schema: MessageType = reader.getFooter.getFileMetaData.getSchema
    private val columnIOFactory = new ColumnIOFactory()
    private val elementsAreDouble: Boolean = detectDoubleElement(schema, vectorColumn)
    private val pkIsInt32: Boolean = detectInt32Pk(schema, pkColumn)

    private var pages: PageReadStore = _
    private var recordReader: org.apache.parquet.io.RecordReader[Group] = _
    private var rowsLeftInGroup: Long = 0L
    private var closed: Boolean = false

    advanceRowGroup()

    private def advanceRowGroup(): Unit = {
      pages = reader.readNextRowGroup()
      if (pages == null) {
        closeQuietly()
      } else {
        val columnIO = columnIOFactory.getColumnIO(schema)
        recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
        rowsLeftInGroup = pages.getRowCount
      }
    }

    override def hasNext: Boolean = {
      if (closed) return false
      if (rowsLeftInGroup <= 0) {
        advanceRowGroup()
        if (closed) return false
      }
      true
    }

    override def next(): (Long, Array[Float]) = {
      if (!hasNext) throw new NoSuchElementException("No more rows in parquet stream")
      val group = recordReader.read()
      rowsLeftInGroup -= 1
      val pk = if (pkIsInt32) group.getInteger(pkColumn, 0).toLong
               else group.getLong(pkColumn, 0)
      val vec = readVector(group, vectorColumn, elementsAreDouble)
      (pk, vec)
    }

    private def closeQuietly(): Unit = {
      if (!closed) {
        closed = true
        try reader.close()
        catch { case _: Throwable => () }
      }
    }
  }

  private def readVector(
    group: Group, vectorColumn: String, elementsAreDouble: Boolean
  ): Array[Float] = {
    // Spark writes Array[Float] / Array[Double] as a 3-level LIST:
    //   <col> (LIST) { repeated group list { optional <type> element; } }
    val listGroup = group.getGroup(vectorColumn, 0)
    val dim = listGroup.getFieldRepetitionCount("list")
    val vec = new Array[Float](dim)
    var i = 0
    if (elementsAreDouble) {
      while (i < dim) {
        vec(i) = listGroup.getGroup("list", i).getDouble("element", 0).toFloat
        i += 1
      }
    } else {
      while (i < dim) {
        vec(i) = listGroup.getGroup("list", i).getFloat("element", 0)
        i += 1
      }
    }
    vec
  }

  /**
   * Inspect the parquet schema to determine whether the vector element
   * type is DOUBLE (Python list path) or FLOAT (Scala Array[Float] path).
   * Throws IllegalArgumentException for any other element type.
   */
  private def detectDoubleElement(schema: MessageType, vectorColumn: String): Boolean = {
    import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
    // MessageType.getType has both single-arg and varargs overloads in
    // recent parquet versions; calling via getFields().find avoids the
    // ambiguous-overload compile error at the cost of a tiny scan.
    val fieldOpt = {
      val fields = schema.getFields
      var i = 0
      var found: Option[org.apache.parquet.schema.Type] = None
      while (i < fields.size() && found.isEmpty) {
        val f = fields.get(i)
        if (f.getName == vectorColumn) found = Some(f)
        i += 1
      }
      found
    }
    val listType = fieldOpt
      .getOrElse(throw new IllegalArgumentException(
        s"Column '$vectorColumn' not found in parquet schema"))
      .asGroupType()
    val elementType = listType.getType("list").asGroupType().getType("element")
    if (!elementType.isPrimitive) {
      throw new IllegalArgumentException(
        s"Column '$vectorColumn' element is not a primitive: ${elementType.toString}"
      )
    }
    elementType.asPrimitiveType().getPrimitiveTypeName match {
      case PrimitiveTypeName.FLOAT  => false
      case PrimitiveTypeName.DOUBLE => true
      case other =>
        throw new IllegalArgumentException(
          s"Column '$vectorColumn' must be FLOAT or DOUBLE, got $other"
        )
    }
  }

  /**
   * Inspect the parquet schema to determine whether the pk column is
   * INT32 (true) or INT64 (false). Any other type is rejected with a
   * message pointing the caller at the future mapping-table support.
   */
  private def detectInt32Pk(schema: MessageType, pkColumn: String): Boolean = {
    import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
    val fields = schema.getFields
    var i = 0
    var found: Option[org.apache.parquet.schema.Type] = None
    while (i < fields.size() && found.isEmpty) {
      val f = fields.get(i)
      if (f.getName == pkColumn) found = Some(f)
      i += 1
    }
    val field = found.getOrElse(
      throw new IllegalArgumentException(
        s"Pk column '$pkColumn' not found in parquet schema"
      )
    )
    if (!field.isPrimitive) {
      throw new IllegalArgumentException(
        s"Pk column '$pkColumn' must be a primitive INT32 or INT64, got ${field.toString}"
      )
    }
    field.asPrimitiveType().getPrimitiveTypeName match {
      case PrimitiveTypeName.INT32 => true
      case PrimitiveTypeName.INT64 => false
      case other =>
        throw new IllegalArgumentException(
          s"Pk column '$pkColumn' must be INT32 or INT64, got $other. " +
            "String / UUID pk columns will be supported via a mapping table " +
            "in a future release; for now, project to a Long pk (e.g. via hash + monotonic_id)."
        )
    }
  }
}
