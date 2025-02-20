import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream}
import java.util.concurrent.ThreadLocalRandom

import scala.collection.JavaConverters._
import scala.collection.mutable

import ai.rapids.cudf._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.kudo.{ByteArrayOutputStreamWriter, KudoSerializer, KudoTable, KudoTableHeader, OpenByteArrayOutputStream}
import java.util
import org.scalatest.funsuite.AnyFunSuite

class KudoSerializerTest extends AnyFunSuite {

  test("Serialize and Deserialize Table") {
    try {
      val expected = buildTestTable()
      val rowCount = expected.getRowCount.toInt
      for (sliceSize <- 1 to rowCount) {
        val tableSlices = new mutable.ListBuffer[TableSlice]()
        for (startRow <- 0 until rowCount by sliceSize) {
          tableSlices += TableSlice(startRow, Math.min(sliceSize, rowCount - startRow), expected)
        }
        checkMergeTable(expected, tableSlices)
      }
    } catch {
      case e: Exception => throw new RuntimeException(e)
    }
  }

  test("Row Count Only") {
    val out = new OpenByteArrayOutputStream()
    val bytesWritten = KudoSerializer.writeRowCountToStream(out, 5)
    assert(bytesWritten == 28)

    val in = new ByteArrayInputStream(out.toByteArray)
    val header = KudoTableHeader.readFrom(new DataInputStream(in)).get()

    assert(header.getNumColumns == 0)
    assert(header.getOffset == 0)
    assert(header.getNumRows == 5)
    assert(header.getValidityBufferLen == 0)
    assert(header.getOffsetBufferLen == 0)
    assert(header.getTotalDataLen == 0)
  }

  test("Write Simple") {
    val serializer = new KudoSerializer(buildSimpleTestSchema())

    withResource(buildSimpleTable())( t => {
      val out = new OpenByteArrayOutputStream()
      val bytesWritten = serializer.writeToStreamWithMetrics(t, out, 0, 4).getWrittenBytes
      assert(bytesWritten == 189)

      val in = new ByteArrayInputStream(out.toByteArray)
      val header = KudoTableHeader.readFrom(new DataInputStream(in)).get()
      assert(header.getNumColumns == 7)
      assert(header.getOffset == 0)
      assert(header.getNumRows == 4)
      assert(header.getValidityBufferLen == 24)
      assert(header.getOffsetBufferLen == 40)
      assert(header.getTotalDataLen == 160)

      // First integer column has no validity buffer
      assert(!header.hasValidityBuffer(0))
      for (i <- 1 until 7) {
        assert(header.hasValidityBuffer(i))
      }
    })
  }

  test("Merge Table With Different Validity") {
    withResource(new mutable.ArrayBuffer[Table]()) { tables => {
      val table1 = new Table.TestBuilder()
        .column(Long.box(-83182L), 5822L, 3389L, 7384L, 7297L)
        .column(-2.06, -2.14, 8.04, 1.16, -1.0)
        .build()
      tables.+=(table1)

      val table2 = new Table.TestBuilder()
        .column(Long.box(-47L), null, -83L, -166L, -220L, 470L, 619L, 803L, 661L)
        .column(-6.08, 1.6, 1.78, -8.01, 1.22, 1.43, 2.13, -1.65, null)
        .build()
      tables.+=(table2)

      val table3 = new Table.TestBuilder()
        .column(Long.box(8722L), 8733L)
        .column(2.51, 0.0)
        .build()
      tables.+=(table3)

      val expected = new Table.TestBuilder()
        .column(Long.box(7384L), 7297L, 803L, 661L, 8733L)
        .column(1.16, -1.0, -1.65, null, 0.0)
        .build()
      tables.+=(expected)

      checkMergeTable(expected, Seq(
        new TableSlice(3, 2, table1),
        new TableSlice(7, 2, table2),
        new TableSlice(1, 1, table3)
      ))
      null
    }
    }
  }

  test("Merge String") {
    withResource(new mutable.ArrayBuffer[Table]()) { tables => {
      val table1 = new Table.TestBuilder()
        .column("A", "B", "C", "D", null, "TESTING", "1", "2", "3", "4", "5", "6", "7",
          null, "9", "10", "11", "12", "13", null, "15")
        .build()
      tables.+=(table1)

      val table2 = new Table.TestBuilder()
        .column("A", "A", "C", "C", "E", "TESTING", "1", "2", "3", "4", "5", "6", "7",
          "", "9", "10", "11", "12", "13", "", "15")
        .build()
      tables.+=(table2)

      val expected = new Table.TestBuilder()
        .column("C", "D", null, "TESTING", "1", "2", "3", "4", "5", "6", "7",
          null, "9", "C", "E", "TESTING", "1", "2")
        .build()
      tables.+=(expected)

      checkMergeTable(expected, Seq(
        new TableSlice(2, 13, table1),
        new TableSlice(3, 5, table2)
      ))
      null
    }
    }
  }

  test("Merge List") {
    withResource(new mutable.ArrayBuffer[Table]()) { tables => {

      val table1 = new Table.TestBuilder()
        .column(Long.box(-881L), 482L, 660L, 896L, -129L, -108L, -428L, 0L, 617L, 782L)
        .column(integers(665), integers(-267), integers(398), integers(-314),
          integers(-370), integers(181), integers(665, 544), integers(222),
          integers(-587), integers(544))
        .build()
      tables.+=(table1)

      val table2 = new Table.TestBuilder()
        .column(Long.box(-881L), 482L, 660L, 896L, 122L, 241L, 281L, 680L, 783L, null)
        .column(integers(-370), integers(398), integers(-587, 398), integers(-314), integers(307),
          integers(-397, -633), integers(-314, 307), integers(-633), integers(-397),
          integers(181, -919, -175))
        .build()
      tables.+=(table2)

      val expected = new Table.TestBuilder()
        .column(Long.box(896L), -129L, -108L, -428L, 0L, 617L, 782L, 482L,
          660L, 896L, 122L, 241L, 281L, 680L, 783L, null)
        .column(integers(-314), integers(-370), integers(181), integers(665, 544),
          integers(222), integers(-587), integers(544), integers(398), integers(-587, 398),
          integers(-314), integers(307), integers(-397, -633), integers(-314, 307),
          integers(-633), integers(-397), integers(181, -919, -175))
        .build()
      tables.+=(expected)

      checkMergeTable(expected, Seq(
        new TableSlice(3, 7, table1),
        new TableSlice(1, 9, table2)
      ))
      null
    }
    }
  }

  test("Merge Complex Struct List") {
    withResource(new mutable.ArrayBuffer[Table]()) { tables => {

      val listMapType = new HostColumnVector.ListType(true,
        new HostColumnVector.ListType(true,
          new HostColumnVector.StructType(true,
            new HostColumnVector.BasicType(false, DType.STRING),
            new HostColumnVector.BasicType(true, DType.STRING))))

      val table = new Table.TestBuilder()
        .column(listMapType,
          List(
            List(struct("k1", "v1"), struct("k2", "v2")).asJava,
            List(struct("k3", "v3")).asJava).asJava,
          null,
          List(
            List(struct("k14", "v14"), struct("k15", "v15")).asJava
          ).asJava,
          null,
          List(null, null, null).asJava,
          List(
            List(struct("k22", null)).asJava,
            List(struct("k23", null)).asJava
          ).asJava, null, null, null)
        .build()
      tables.+=(table)

      checkMergeTable(table, Seq(
        new TableSlice(0, 3, table),
        new TableSlice(3, 3, table),
        new TableSlice(6, 3, table)
      ))
      null
    }
    }
  }

  test("Serialize Validity") {
    withResource(new mutable.ArrayBuffer[Table]()) { tables => {

      val col1 = mutable.ListBuffer[Integer](512)
      col1.+=(null)
      col1.+=(null)
      2 until 512 foreach { i => col1.+=(i) }

      val table1 = new Table.TestBuilder()
        .column(col1.toArray)
        .build()
      tables.+=(table1)

      val table2 = new Table.TestBuilder()
        .column(Int.box(509), 510, 511)
        .build()
      tables.+=(table2)

      checkMergeTable(table2, Seq(new TableSlice(509, 3, table1)))
      null
    }
    }
  }

  test("ByteArrayOutputStreamWriter") {
    val bout = new ByteArrayOutputStream(32)
    val writer = new ByteArrayOutputStreamWriter(bout)

    writer.writeInt(0x12345678)

    val testByteArr1 = new Array[Byte](2097)
    ThreadLocalRandom.current().nextBytes(testByteArr1)
    writer.write(testByteArr1, 0, testByteArr1.length)

    val testByteArr2 = new Array[Byte](7896)
    ThreadLocalRandom.current().nextBytes(testByteArr2)
    val buffer = HostMemoryBuffer.allocate(testByteArr2.length)
    try {
      buffer.setBytes(0, testByteArr2, 0, testByteArr2.length)
      writer.copyDataFrom(buffer, 0, testByteArr2.length)
    } finally {
      buffer.close()
    }

    val expected = new Array[Byte](4 + testByteArr1.length + testByteArr2.length)
    expected(0) = 0x12.toByte
    expected(1) = 0x34.toByte
    expected(2) = 0x56.toByte
    expected(3) = 0x78.toByte
    System.arraycopy(testByteArr1, 0, expected, 4, testByteArr1.length)
    System.arraycopy(testByteArr2, 0, expected, 4 + testByteArr1.length, testByteArr2.length)

    assert(expected.sameElements(bout.toByteArray))
  }

  // Helper methods and classes
  private def buildSimpleTestSchema(): Schema = {
    val builder = Schema.builder()
    builder.addColumn(DType.INT32, "a")
    builder.addColumn(DType.STRING, "b")
    val listBuilder = builder.addColumn(DType.LIST, "c")
    listBuilder.addColumn(DType.INT32, "c1")
    val structBuilder = builder.addColumn(DType.STRUCT, "d")
    structBuilder.addColumn(DType.INT8, "d1")
    structBuilder.addColumn(DType.INT64, "d2")
    builder.build()
  }

  private def buildSimpleTable(): Table = {
    val st = new HostColumnVector.StructType(
      true, new HostColumnVector.BasicType(true, DType.INT8),
      new HostColumnVector.BasicType(true, DType.INT64))
    new Table.TestBuilder()
      .column(Int.box(1), 2, 3, 4)
      .column("1", "12", null, "45")
      .column(Array[Integer](1, null, 3), Array[Integer](4, 5, 6), null, Array[Integer](7, 8, 9))
      .column(st, new HostColumnVector.StructData(Byte.box(1.toByte), Long.box(11L)),
        new HostColumnVector.StructData(Byte.box(2.toByte), null),
        null, new HostColumnVector.StructData(Byte.box(3.toByte), Long.box(33L)))
      .build()
  }

  private def buildTestTable(): Table = {
    val listMapType = new HostColumnVector.ListType(true,
      new HostColumnVector.ListType(true,
        new HostColumnVector.StructType(true,
          new HostColumnVector.BasicType(false, DType.STRING),
          new HostColumnVector.BasicType(true, DType.STRING))))
    val mapStructType = new HostColumnVector.ListType(true,
      new HostColumnVector.StructType(true,
        new HostColumnVector.BasicType(false, DType.STRING),
        new HostColumnVector.BasicType(false, DType.STRING)))
    val structType = new HostColumnVector.StructType(true,
      new HostColumnVector.BasicType(true, DType.INT32),
      new HostColumnVector.BasicType(false, DType.FLOAT32))
    val listDateType = new HostColumnVector.ListType(true,
      new HostColumnVector.StructType(false,
        new HostColumnVector.BasicType(false, DType.INT32),
        new HostColumnVector.BasicType(true, DType.INT32)))

    new Table.TestBuilder()
      .column(Int.box(100), 202, 3003, 40004, 5, -60, 1, null, 3, null, 5, null, 7, null, 9, null,
        11, null, 13, null, 15)
      .column(true, true, false, false, true, null, true, true, null, false, false, null, true,
        true, null, false, false, null, true, true, null)
      .column(Byte.box(1.toByte), 2.toByte, null, 4.toByte, 5.toByte, 6.toByte, 1.toByte, 2.toByte,
        3.toByte,
        null, 5.toByte, 6.toByte,
        7.toByte, null, 9.toByte, 10.toByte, 11.toByte, null, 13.toByte, 14.toByte, 15.toByte)
      .column(Short.box(6.toShort), 5.toShort, 4.toShort, null, 2.toShort, 1.toShort,
        1.toShort, 2.toShort, 3.toShort, null, 5.toShort, 6.toShort, 7.toShort, null, 9.toShort,
        10.toShort, null, 12.toShort, 13.toShort, 14.toShort, null)
      .column(Long.box(1L), null, 1001L, 50L, -2000L, null, 1L, 2L, 3L, 4L, null, 6L,
        7L, 8L, 9L, null, 11L, 12L, 13L, 14L, null)
      .column(Float.box(10.1f), 20f, -1f, 3.1415f, -60f, null, 1f, 2f, 3f, 4f, 5f, null, 7f, 8f,
        9f, 10f, 11f,
        null, 13f, 14f, 15f)
      .column(Float.box(10.1f), 20f, -2f, 3.1415f, -60f, -50f, 1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f,
        9f, 10f, 11f,
        12f, 13f, 14f, 15f)
      .column(10.1, 20.0, 33.1, 3.1415, -60.5, null, 1d, 2.0, 3.0, 4.0, 5.0,
        6.0, null, 8.0, 9.0, 10.0, 11.0, 12.0, null, 14.0, 15.0)
      .column(Float.box(null.asInstanceOf[Float]), null, null, null, null, null, null,
        null, null, null,
        null, null, null, null, null, null, null, null, null, null, null)
      .timestampDayColumn(99, 100, 101, 102, 103, 104, 1, 2, 3, 4, 5, 6, 7, null, 9, 10,
        11, 12, 13,
        null, 15)
      .timestampMillisecondsColumn(9L, 1006L, 101L, 5092L, null, 88L, 1L, 2L, 3L, 4L, 5L, 6L, 7L,
        8L, null, 10L, 11L, 12L, 13L, 14L, 15L)
      .timestampSecondsColumn(1L, null, 3L, 4L, 5L, 6L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, null,
        11L, 12L, 13L, 14L, 15L)
      .decimal32Column(-3, 100, 202, 3003, 40004, 5, -60, 1, null, 3,
        null, 5, null, 7, null, 9, null, 11, null, 13, null, 15)
      .decimal64Column(-8, 1L, null, 1001L, 50L, -2000L, null, 1L, 2L, 3L,
        4L, null, 6L, 7L, 8L, 9L, null, 11L, 12L, 13L, 14L, null)
      .column("A", "B", "C", "D", null, "TESTING", "1", "2", "3", "4",
        "5", "6", "7", null, "9", "10", "11", "12", "13", null, "15")
      .column("A", "A", "C", "C", "E", "TESTING", "1", "2", "3", "4", "5",
        "6", "7", "", "9", "10", "11", "12", "13", "", "15")
      .column("", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
        "", "", "", "", "", "")
      .column("", null, "", "", null, "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
        "")
      .column(null.asInstanceOf[String], null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null)
      .column(mapStructType, structs(struct("1", "2")), structs(struct("3", "4")), null, null,
        structs(struct("key", "value"), struct("a", "b")), null, null,
        structs(struct("3", "4"), struct("1", "2")), structs(),
        structs(null, struct("foo", "bar")),
        structs(null, null, null), null, null, null, null, null, null, null, null, null,
        structs(struct("the", "end")))
      .column(structType, struct(1, 1f), null, struct(2, 3f),
        null, struct(8, 7f), struct(0, 0f), null,
        null, struct(-1, -1f), struct(-100, -100f),
        struct(Int.MaxValue, Float.MaxValue), null,
        null, null,
        null, null,
        null, null,
        null, null,
        struct(Int.MinValue, Float.MinValue))
      .column(integers(1, 2), null, integers(3, 4, null, 5, null), null, null, integers(6, 7, 8),
        integers(null, null, null), integers(1, 2, 3), integers(4, 5, 6), integers(7, 8, 9),
        integers(10, 11, 12), integers(null), integers(14, null),
        integers(14, 15, null, 16, 17, 18), integers(19, 20, 21), integers(22, 23, 24),
        integers(25, 26, 27), integers(28, 29, 30), integers(31, 32, 33), null,
        integers(37, 38, 39))
      .column(integers(), integers(), integers(), integers(), integers(), integers(), integers(),
        integers(), integers(), integers(), integers(), integers(), integers(), integers(),
        integers(), integers(), integers(), integers(), integers(), integers(), integers())
      .column(integers(null, null), integers(null, null, null, null), integers(),
        integers(null, null, null), integers(), integers(null, null, null, null, null),
        integers(null), integers(null, null, null), integers(null, null),
        integers(null, null, null, null), integers(null, null, null, null, null), integers(),
        integers(null, null, null, null), integers(null, null, null), integers(null, null),
        integers(null, null, null), integers(null, null), integers(null),
        integers(null), integers(null, null),
        integers(null, null, null, null, null))
      .column(null.asInstanceOf[Integer], null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null)
      .column(strings("1", "2", "3"), strings("4"), strings("5"), strings("6, 7"),
        strings("", "9", null), strings("11"), strings(""), strings(null, null),
        strings("15", null), null, null, strings("18", "19", "20"), null, strings("22"),
        strings("23", ""), null, null, null, null, strings(), strings("the end"))
      .column(strings(), strings(), strings(), strings(), strings(), strings(), strings(),
        strings(), strings(), strings(), strings(), strings(), strings(), strings(), strings(),
        strings(), strings(), strings(), strings(), strings(), strings())
      .column(strings(null, null), strings(null, null, null, null), strings(),
        strings(null, null, null), strings(), strings(null, null, null, null, null),
        strings(null), strings(null, null, null), strings(null, null),
        strings(null, null, null, null), strings(null, null, null, null, null), strings(),
        strings(null, null, null, null), strings(null, null, null), strings(null, null),
        strings(null, null, null), strings(null, null), strings(null),
        strings(null), strings(null, null),
        strings(null, null, null, null, null))
      .column(null.asInstanceOf[String], null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null)
      .column(
        listMapType,
        List(
          List(struct("k1", "v1"), struct("k2", "v2")).asJava,
          List(struct("k3", "v3")).asJava
        ).asJava,
        List(
          List(
            struct("k4", "v4"),
            struct("k5", "v5"),
            struct("k6", "v6")
          ).asJava,
          List(struct("k7", "v7")).asJava
        ).asJava,
        null,
        null,
        null,
        List(
          List(struct("k8", "v8"), struct("k9", "v9")).asJava,
          List(
            struct("k10", "v10"),
            struct("k11", "v11"),
            struct("k12", "v12"),
            struct("k13", "v13")
          ).asJava
        ).asJava,
        List(
          List(struct("k14", "v14"), struct("k15", "v15")).asJava
        ).asJava,
        null,
        null,
        null,
        null,
        List(
          List(struct("k16", "v16"), struct("k17", "v17")).asJava,
          List(struct("k18", "v18")).asJava
        ).asJava,
        List(
          List(struct("k19", "v19"), struct("k20", "v20")).asJava,
          List(struct("k21", "v21")).asJava
        ).asJava,
        List(
          List(struct("k22", "v22")).asJava,
          List(struct("k23", "v23")).asJava
        ).asJava,
        List(null, null, null).asJava,
        List(
          List(struct("k22", null)).asJava,
          List(struct("k23", null)).asJava
        ).asJava,
        null,
        null,
        null,
        null,
        null
      )
      .column(
        listDateType,
        List(
          struct(-210, 293),
          struct(-719, 205),
          struct(-509, 183),
          struct(174, 122),
          struct(647, 683)
        ).asJava,
        List(
          struct(311, 992),
          struct(-169, 482),
          struct(166, 525)
        ).asJava,
        List(
          struct(156, 197),
          struct(926, 134),
          struct(747, 312),
          struct(293, 801)
        ).asJava,
        List(
          struct(647, null),
          struct(293, 387)
        ).asJava,
        List.empty.asJava,
        null,
        List.empty.asJava,
        null,
        List(
          struct(-210, 293),
          struct(-719, 205),
          struct(-509, 183),
          struct(174, 122),
          struct(647, 683)
        ).asJava,
        List(
          struct(311, 992),
          struct(-169, 482),
          struct(166, 525)
        ).asJava,
        List(
          struct(156, 197),
          struct(926, 134),
          struct(747, 312),
          struct(293, 801)
        ).asJava,
        List(
          struct(647, null),
          struct(293, 387)
        ).asJava,
        List.empty.asJava,
        null,
        List.empty.asJava,
        null,
        List(struct(778, 765)).asJava,
        List(
          struct(7, 87),
          struct(8, 96)
        ).asJava,
        List(
          struct(9, 56),
          struct(10, 532),
          struct(11, 456)
        ).asJava,
        null,
        List.empty.asJava
      )
      .build()
  }

  case class TableSlice(startRow: Int, numRows: Int, baseTable: Table) {}

  private def checkMergeTable(expected: Table, tableSlices: Seq[TableSlice]): Unit = {
    try {
      val serializer = new KudoSerializer(schemaOf(expected))

      val bout = new OpenByteArrayOutputStream()
      for (slice <- tableSlices) {
        serializer.writeToStreamWithMetrics(slice.baseTable, bout, slice.startRow, slice.numRows)
      }
      bout.flush()

      val bin = new ByteArrayInputStream(bout.toByteArray)
      withResource(new mutable.ArrayBuffer[KudoTable](tableSlices.size)) { kudoTables => {
        try {
          for (i <- tableSlices.indices) {
            kudoTables.+=(KudoTable.from(bin).get)
          }

          val rows = kudoTables.map(t => t.getHeader.getNumRows).sum
          assert(expected.getRowCount === rows)

          val merged = serializer.mergeToTable(kudoTables.toArray)
          try {
            assert(expected.getRowCount === merged.getRowCount)
            AssertUtils.assertTablesAreEqual(expected, merged)
          } finally {
            merged.close()
          }
        } catch {
          case e: Exception => throw new RuntimeException(e)
        }

        null
      }
      }
    } catch {
      case e: Exception => throw new RuntimeException(e)
    }
  }

  def integers(values: Integer*): Array[Integer] = values.toArray

  def struct(values: Any*): HostColumnVector.StructData =
    new HostColumnVector.StructData(List(values).asJava)

  def structs(values: HostColumnVector.StructData*): util.List[HostColumnVector.StructData] =
    new util.ArrayList[HostColumnVector.StructData](values.asJava)

  def strings(values: String*): Array[String] = values.toArray

  def schemaOf(t: Table): Schema = {
    val builder = Schema.builder()

    for (i <- 0 until t.getNumberOfColumns) {
      val cv = t.getColumn(i)
      addToSchema(cv, s"col_${i}_", builder)
    }

    builder.build()
  }

  private def addToSchema(cv: ColumnView, namePrefix: String, builder: Schema.Builder): Unit = {
    toSchemaInner(cv, 0, namePrefix, builder)
  }

  private def toSchemaInner(cv: ColumnView, idx: Int, namePrefix: String,
      builder: Schema.Builder): Int = {
    val name = s"$namePrefix$idx"

    val thisBuilder = builder.addColumn(cv.getType, name)
    var lastIdx = idx
    for (i <- 0 until cv.getNumChildren) {
      lastIdx = toSchemaInner(cv.getChildColumnView(i), lastIdx + 1, namePrefix, thisBuilder)
    }

    lastIdx
  }

}
