package com.nvidia.spark.rapids.kudo;

import ai.rapids.cudf.AssertUtils;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.Schema;
import ai.rapids.cudf.Table;
import com.nvidia.spark.rapids.jni.Arms;
import com.nvidia.spark.rapids.jni.kudo.KudoSerializer;
import com.nvidia.spark.rapids.jni.kudo.KudoSerializerTest;
import com.nvidia.spark.rapids.jni.kudo.KudoTable;
import com.nvidia.spark.rapids.jni.kudo.OpenByteArrayOutputStream;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.toIntExact;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class KudoSerializerUtils {


  static Schema buildSimpleTestSchema() {
    Schema.Builder builder = Schema.builder();

    builder.addColumn(DType.INT32, "a");
    builder.addColumn(DType.STRING, "b");
    Schema.Builder listBuilder = builder.addColumn(DType.LIST, "c");
    listBuilder.addColumn(DType.INT32, "c1");

    Schema.Builder structBuilder = builder.addColumn(DType.STRUCT, "d");
    structBuilder.addColumn(DType.INT8, "d1");
    structBuilder.addColumn(DType.INT64, "d2");

    return builder.build();
  }

  static Table buildSimpleTable() {
    HostColumnVector.StructType st = new HostColumnVector.StructType(
        true,
        new HostColumnVector.BasicType(true, DType.INT8),
        new HostColumnVector.BasicType(true, DType.INT64)
    );
    return new Table.TestBuilder()
        .column(1, 2, 3, 4)
        .column("1", "12", null, "45")
        .column(new Integer[]{1, null, 3}, new Integer[]{4, 5, 6}, null, new Integer[]{7, 8, 9})
        .column(st, new HostColumnVector.StructData((byte) 1, 11L),
            new HostColumnVector.StructData((byte) 2, null), null,
            new HostColumnVector.StructData((byte) 3, 33L))
        .build();
  }

  static Table buildTestTable() {
    HostColumnVector.ListType listMapType = new HostColumnVector.ListType(true,
        new HostColumnVector.ListType(true,
            new HostColumnVector.StructType(true,
                new HostColumnVector.BasicType(false, DType.STRING),
                new HostColumnVector.BasicType(true, DType.STRING))));
    HostColumnVector.ListType mapStructType = new HostColumnVector.ListType(true,
        new HostColumnVector.StructType(true,
            new HostColumnVector.BasicType(false, DType.STRING),
            new HostColumnVector.BasicType(false, DType.STRING)));
    HostColumnVector.StructType structType = new HostColumnVector.StructType(true,
        new HostColumnVector.BasicType(true, DType.INT32),
        new HostColumnVector.BasicType(false, DType.FLOAT32));
    HostColumnVector.ListType listDateType = new HostColumnVector.ListType(true,
        new HostColumnVector.StructType(false,
            new HostColumnVector.BasicType(false, DType.INT32),
            new HostColumnVector.BasicType(true, DType.INT32)));

    return new Table.TestBuilder()
        .column(100, 202, 3003, 40004, 5, -60, 1, null, 3, null, 5, null, 7, null, 9, null, 11, null, 13, null, 15)
        .column(true, true, false, false, true, null, true, true, null, false, false, null, true,
            true, null, false, false, null, true, true, null)
        .column((byte)1, (byte)2, null, (byte)4, (byte)5,(byte)6,(byte)1,(byte)2,(byte)3, null,(byte)5, (byte)6,
            (byte) 7, null,(byte) 9,(byte) 10,(byte) 11, null,(byte) 13,(byte) 14,(byte) 15)
        .column((short)6, (short)5, (short)4, null, (short)2, (short)1,
            (short)1, (short)2, (short)3, null, (short)5, (short)6, (short)7, null, (short)9,
            (short)10, null, (short)12, (short)13, (short)14, null)
        .column(1L, null, 1001L, 50L, -2000L, null, 1L, 2L, 3L, 4L, null, 6L,
            7L, 8L, 9L, null, 11L, 12L, 13L, 14L, null)
        .column(10.1f, 20f, -1f, 3.1415f, -60f, null, 1f, 2f, 3f, 4f, 5f, null, 7f, 8f, 9f, 10f, 11f, null, 13f, 14f, 15f)
        .column(10.1f, 20f, -2f, 3.1415f, -60f, -50f, 1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, 11f, 12f, 13f, 14f, 15f)
        .column(10.1, 20.0, 33.1, 3.1415, -60.5, null, 1d, 2.0, 3.0, 4.0, 5.0,
            6.0, null, 8.0, 9.0, 10.0, 11.0, 12.0, null, 14.0, 15.0)
        .column((Float)null, null, null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null, null, null, null)
        .timestampDayColumn(99, 100, 101, 102, 103, 104, 1, 2, 3, 4, 5, 6, 7, null, 9, 10, 11, 12, 13, null, 15)
        .timestampMillisecondsColumn(9L, 1006L, 101L, 5092L, null, 88L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, null, 10L, 11L, 12L, 13L, 14L, 15L)
        .timestampSecondsColumn(1L, null, 3L, 4L, 5L, 6L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, null, 11L, 12L, 13L, 14L, 15L)
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
        .column("", null, "", "", null, "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
        .column((String)null, null, null, null, null, null, null, null, null, null,
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
            struct(Integer.MAX_VALUE, Float.MAX_VALUE), null,
            null, null,
            null, null,
            null, null,
            null, null,
            struct(Integer.MIN_VALUE, Float.MIN_VALUE))
        .column(integers(1, 2), null, integers(3, 4, null, 5, null), null, null, integers(6, 7, 8),
            integers(null, null, null), integers(1, 2, 3), integers(4, 5, 6), integers(7, 8, 9),
            integers(10, 11, 12), integers((Integer)null), integers(14, null),
            integers(14, 15, null, 16, 17, 18), integers(19, 20, 21), integers(22, 23, 24),
            integers(25, 26, 27), integers(28, 29, 30), integers(31, 32, 33), null,
            integers(37, 38, 39))
        .column(integers(), integers(), integers(), integers(), integers(), integers(), integers(),
            integers(), integers(), integers(), integers(), integers(), integers(), integers(),
            integers(), integers(), integers(), integers(), integers(), integers(), integers())
        .column(integers(null, null), integers(null, null, null, null), integers(),
            integers(null, null, null), integers(), integers(null, null, null, null, null),
            integers((Integer)null), integers(null, null, null), integers(null, null),
            integers(null, null, null, null), integers(null, null, null, null, null), integers(),
            integers(null, null, null, null), integers(null, null, null), integers(null, null),
            integers(null, null, null), integers(null, null), integers((Integer)null),
            integers((Integer)null), integers(null, null),
            integers(null, null, null, null, null))
        .column((Integer)null, null, null, null, null, null, null, null, null, null,
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
            strings((String)null), strings(null, null, null), strings(null, null),
            strings(null, null, null, null), strings(null, null, null, null, null), strings(),
            strings(null, null, null, null), strings(null, null, null), strings(null, null),
            strings(null, null, null), strings(null, null), strings((String)null),
            strings((String)null), strings(null, null),
            strings(null, null, null, null, null))
        .column((String)null, null, null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null, null, null, null)
        .column(listMapType, asList(asList(struct("k1", "v1"), struct("k2", "v2")),
                singletonList(struct("k3", "v3"))),
            asList(asList(struct("k4", "v4"), struct("k5", "v5"),
                struct("k6", "v6")), singletonList(struct("k7", "v7"))),
            null, null, null, asList(asList(struct("k8", "v8"), struct("k9", "v9")),
                asList(struct("k10", "v10"), struct("k11", "v11"), struct("k12", "v12"),
                    struct("k13", "v13"))),
            singletonList(asList(struct("k14", "v14"), struct("k15", "v15"))), null, null, null, null,
            asList(asList(struct("k16", "v16"), struct("k17", "v17")),
                singletonList(struct("k18", "v18"))),
            asList(asList(struct("k19", "v19"), struct("k20", "v20")),
                singletonList(struct("k21", "v21"))),
            asList(singletonList(struct("k22", "v22")), singletonList(struct("k23", "v23"))),
            asList(null, null, null),
            asList(singletonList(struct("k22", null)), singletonList(struct("k23", null))),
            null, null, null, null, null)
        .column(listDateType, asList(struct(-210, 293), struct(-719, 205), struct(-509, 183),
                struct(174, 122), struct(647, 683)),
            asList(struct(311, 992), struct(-169, 482), struct(166, 525)),
            asList(struct(156, 197), struct(926, 134), struct(747, 312), struct(293, 801)),
            asList(struct(647, null), struct(293, 387)), emptyList(),
            null, emptyList(), null,
            asList(struct(-210, 293), struct(-719, 205), struct(-509, 183), struct(174, 122),
                struct(647, 683)),
            asList(struct(311, 992), struct(-169, 482), struct(166, 525)),
            asList(struct(156, 197), struct(926, 134), struct(747, 312), struct(293, 801)),
            asList(struct(647, null), struct(293, 387)), emptyList(), null,
            emptyList(), null,
            singletonList(struct(778, 765)), asList(struct(7, 87), struct(8, 96)),
            asList(struct(9, 56), struct(10, 532), struct(11, 456)), null, emptyList())
        .build();
  }
//
//  static void checkMergeTable(Table expected, List<TableSlice> tableSlices) {
//    try {
//      com.nvidia.spark.rapids.jni.kudo.KudoSerializer serializer = new KudoSerializer(schemaOf(expected));
//
//      com.nvidia.spark.rapids.jni.kudo.OpenByteArrayOutputStream bout = new OpenByteArrayOutputStream();
//      for (TableSlice slice : tableSlices) {
//        serializer.writeToStreamWithMetrics(slice.getBaseTable(), bout, slice.getStartRow(), slice.getNumRows());
//      }
//      bout.flush();
//
//      ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
//      Arms.withResource(new ArrayList<com.nvidia.spark.rapids.jni.kudo.KudoTable>(tableSlices.size()), kudoTables -> {
//        try {
//          for (int i = 0; i < tableSlices.size(); i++) {
//            kudoTables.add(com.nvidia.spark.rapids.jni.kudo.KudoTable.from(bin).get());
//          }
//
//          long rows = kudoTables.stream().mapToLong(t -> t.getHeader().getNumRows()).sum();
//          assertEquals(expected.getRowCount(), toIntExact(rows));
//
//          try (Table merged = serializer.mergeToTable(kudoTables.toArray(new KudoTable[0]))) {
//            assertEquals(expected.getRowCount(), merged.getRowCount());
//
//            AssertUtils.assertTablesAreEqual(expected, merged);
//          }
//        } catch (Exception e) {
//          throw new RuntimeException(e);
//        }
//
//        return null;
//      });
//    } catch (Exception e) {
//      throw new RuntimeException(e);
//    }
//  }

  static Integer[] integers(Integer... values) {
    return values;
  }

  static HostColumnVector.StructData struct(Object... values) {
    return new HostColumnVector.StructData(values);
  }

  static List<HostColumnVector.StructData> structs(HostColumnVector.StructData... values) {
    return asList(values);
  }

  static String[] strings(String... values) {
    return values;
  }

  static Schema schemaOf(Table t) {
    Schema.Builder builder = Schema.builder();

    for (int i = 0; i < t.getNumberOfColumns(); i++) {
      ColumnVector cv = t.getColumn(i);
      addToSchema(cv, "col_" + i + "_", builder);
    }

    return builder.build();
  }

  static void addToSchema(ColumnView cv, String namePrefix, Schema.Builder builder) {
    toSchemaInner(cv, 0, namePrefix, builder);
  }

  static int toSchemaInner(ColumnView cv, int idx, String namePrefix,
      Schema.Builder builder) {
    String name = namePrefix + idx;

    Schema.Builder thisBuilder = builder.addColumn(cv.getType(), name);
    int lastIdx = idx;
    for (int i = 0; i < cv.getNumChildren(); i++) {
      lastIdx = toSchemaInner(cv.getChildColumnView(i), lastIdx + 1, namePrefix,
          thisBuilder);
    }

    return lastIdx;
  }

//
//  static class TableSlice {
//    private final int startRow;
//    private final int numRows;
//    private final Table baseTable;
//
//    private TableSlice(int startRow, int numRows, Table baseTable) {
//      this.startRow = startRow;
//      this.numRows = numRows;
//      this.baseTable = baseTable;
//    }
//
//    public int getStartRow() {
//      return startRow;
//    }
//
//    public int getNumRows() {
//      return numRows;
//    }
//
//    public Table getBaseTable() {
//      return baseTable;
//    }
//  }
}
