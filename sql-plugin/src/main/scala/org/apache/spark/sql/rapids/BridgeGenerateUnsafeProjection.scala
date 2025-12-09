/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.RapidsHostColumnBuilder

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A projection that writes directly to RapidsHostColumnBuilders.
 *
 * CAUTION: the returned projection object should *not* be assumed to be thread-safe.
 */
abstract class BridgeUnsafeProjection {
  def apply(rows: Iterator[InternalRow],
            builders: Array[RapidsHostColumnBuilder]): Unit

  def initialize(partitionIndex: Int): Unit = {}
}

/**
 * Interpreted implementation of BridgeUnsafeProjection.
 * Falls back to direct expression evaluation when code generation fails.
 * Shares the same append logic as GpuCpuBridgeExpression.
 */
class InterpretedBridgeUnsafeProjection(expressions: Seq[Expression]) 
  extends BridgeUnsafeProjection {
  
  // Create optimized append functions once for each expression
  private val appendFunctions = expressions.map { expr =>
    BridgeUnsafeProjection.createOptimizedAppendFunction(expr.dataType, expr.nullable)
  }
  
  override def apply(rows: Iterator[InternalRow],
    builders: Array[RapidsHostColumnBuilder]): Unit = {
    rows.foreach { row =>
      expressions.zipWithIndex.foreach { case (expr, idx) =>
        val result = expr.eval(row)
        appendFunctions(idx)(result, builders(idx))
      }
    }
  }
}

/**
 * The factory object for `UnsafeProjection`.
 */
object BridgeUnsafeProjection
  extends CodeGeneratorWithInterpretedFallback[Seq[Expression], BridgeUnsafeProjection] {

  override protected def createCodeGeneratedObject(in: Seq[Expression]): BridgeUnsafeProjection = {
    // Just call generate directly - let any exceptions propagate naturally
    // The CodeGeneratorWithInterpretedFallback base class will catch exceptions 
    // and fall back to createInterpretedObject
    BridgeGenerateUnsafeProjection.generate(in, SQLConf.get.subexpressionEliminationEnabled)
  }

  override protected def createInterpretedObject(in: Seq[Expression]): BridgeUnsafeProjection = {
    new InterpretedBridgeUnsafeProjection(in)
  }

  /**
   * Creates an optimized append function for the specific data type and nullability.
   * Similar to how TypeConverter generates specialized functions, this avoids the overhead
   * of evaluating the data type on every append operation.
   * 
   * This is SHARED code used by both:
   * - InterpretedBridgeUnsafeProjection (when codegen fails)
   * - GpuCpuBridgeExpression (for the existing interpreted evaluation path)
   */
  def createOptimizedAppendFunction(dataType: DataType, 
    nullable: Boolean): (Any, RapidsHostColumnBuilder) => Unit = {
    import com.nvidia.spark.rapids.GpuRowToColumnConverter
    import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
    
    dataType match {
      // Primitive types - generate specialized functions
      case BooleanType =>
        if (nullable) (value: Any, builder: RapidsHostColumnBuilder) => 
          if (value == null) builder.appendNull() else builder.append(value.asInstanceOf[Boolean])
        else (value: Any, builder: RapidsHostColumnBuilder) => 
          builder.append(value.asInstanceOf[Boolean])
      
      case ByteType =>
        if (nullable) (value: Any, builder: RapidsHostColumnBuilder) => 
          if (value == null) builder.appendNull() else builder.append(value.asInstanceOf[Byte])
        else (value: Any, builder: RapidsHostColumnBuilder) => 
          builder.append(value.asInstanceOf[Byte])
      
      case ShortType =>
        if (nullable) (value: Any, builder: RapidsHostColumnBuilder) => 
          if (value == null) builder.appendNull() else builder.append(value.asInstanceOf[Short])
        else (value: Any, builder: RapidsHostColumnBuilder) => 
          builder.append(value.asInstanceOf[Short])
      
      case IntegerType | DateType =>
        if (nullable) (value: Any, builder: RapidsHostColumnBuilder) => 
          if (value == null) builder.appendNull() else builder.append(value.asInstanceOf[Int])
        else (value: Any, builder: RapidsHostColumnBuilder) => 
          builder.append(value.asInstanceOf[Int])
      
      case LongType | TimestampType =>
        if (nullable) (value: Any, builder: RapidsHostColumnBuilder) => 
          if (value == null) builder.appendNull() else builder.append(value.asInstanceOf[Long])
        else (value: Any, builder: RapidsHostColumnBuilder) => 
          builder.append(value.asInstanceOf[Long])
      
      case FloatType =>
        if (nullable) (value: Any, builder: RapidsHostColumnBuilder) => 
          if (value == null) builder.appendNull() else builder.append(value.asInstanceOf[Float])
        else (value: Any, builder: RapidsHostColumnBuilder) => 
          builder.append(value.asInstanceOf[Float])
      
      case DoubleType =>
        if (nullable) (value: Any, builder: RapidsHostColumnBuilder) => 
          if (value == null) builder.appendNull() else builder.append(value.asInstanceOf[Double])
        else (value: Any, builder: RapidsHostColumnBuilder) => 
          builder.append(value.asInstanceOf[Double])
      
      case StringType =>
        if (nullable) (value: Any, builder: RapidsHostColumnBuilder) => 
          if (value == null) builder.appendNull() else {
            val utf8String = value.asInstanceOf[UTF8String]
            builder.appendUTF8String(utf8String.getBytes)
          }
        else (value: Any, builder: RapidsHostColumnBuilder) => {
          val utf8String = value.asInstanceOf[UTF8String]
          builder.appendUTF8String(utf8String.getBytes)
        }
      
      case BinaryType =>
        if (nullable) (value: Any, builder: RapidsHostColumnBuilder) => 
          if (value == null) {
            builder.appendNull() 
          } else {
            builder.appendByteList(value.asInstanceOf[Array[Byte]])
          }
        else (value: Any, builder: RapidsHostColumnBuilder) => 
          builder.appendByteList(value.asInstanceOf[Array[Byte]])
      
      case _: DecimalType =>
        if (nullable) (value: Any, builder: RapidsHostColumnBuilder) => 
          if (value == null) {
            builder.appendNull() 
          } else {
            builder.append(value.asInstanceOf[Decimal].toJavaBigDecimal)
          }
        else (value: Any, builder: RapidsHostColumnBuilder) => 
          builder.append(value.asInstanceOf[Decimal].toJavaBigDecimal)
      
      case _: YearMonthIntervalType =>
        // YearMonthIntervalType is stored as Int (number of months)
        if (nullable) (value: Any, builder: RapidsHostColumnBuilder) => 
          if (value == null) builder.appendNull() else builder.append(value.asInstanceOf[Int])
        else (value: Any, builder: RapidsHostColumnBuilder) => 
          builder.append(value.asInstanceOf[Int])
      
      case _: DayTimeIntervalType =>
        // DayTimeIntervalType is stored as Long (number of microseconds)
        if (nullable) (value: Any, builder: RapidsHostColumnBuilder) => 
          if (value == null) builder.appendNull() else builder.append(value.asInstanceOf[Long])
        else (value: Any, builder: RapidsHostColumnBuilder) => 
          builder.append(value.asInstanceOf[Long])
      
      case _ =>
        // For complex types or unsupported types, fall back to TypeConverter
        // Only pay the SpecificInternalRow overhead when we actually need it
        val converter = GpuRowToColumnConverter.getConverterForType(dataType, nullable)
        val resultRow = new SpecificInternalRow(Array(dataType))
        
        (value: Any, builder: RapidsHostColumnBuilder) => {
          resultRow.update(0, value)
          converter.append(resultRow, 0, builder)
        }
    }
  }

  /**
   * Returns an UnsafeProjection for given StructType.
   *
   * CAUTION: the returned projection object is *not* thread-safe.
   */
  def create(schema: StructType): BridgeUnsafeProjection = create(schema.fields.map(_.dataType))

  /**
   * Returns an UnsafeProjection for given Array of DataTypes.
   *
   * CAUTION: the returned projection object is *not* thread-safe.
   */
  def create(fields: Array[DataType]): BridgeUnsafeProjection = {
    create(fields.zipWithIndex.map(x => BoundReference(x._2, x._1, true)))
  }

  /**
   * Returns an UnsafeProjection for given sequence of bound Expressions.
   */
  def create(exprs: Seq[Expression]): BridgeUnsafeProjection = {
    createObject(exprs)
  }

  def create(expr: Expression): BridgeUnsafeProjection = create(Seq(expr))

  /**
   * Returns an UnsafeProjection for given sequence of Expressions, which will be bound to
   * `inputSchema`.
   */
  def create(exprs: Seq[Expression], inputSchema: Seq[Attribute]): BridgeUnsafeProjection = {
    create(bindReferences(exprs, inputSchema))
  }
}

/**
 * Generates a [[Projection]] that returns an [[UnsafeRow]].
 *
 * It generates the code for all the expressions, computes the total length for all the columns
 * (can be accessed via variables), and then copies the data into a scratch buffer space in the
 * form of UnsafeRow (the scratch buffer will grow as needed).
 *
 * @note The returned UnsafeRow will be pointed to a scratch buffer inside the projection.
 */
object BridgeGenerateUnsafeProjection extends
  CodeGenerator[Seq[Expression], BridgeUnsafeProjection] {

  case class Schema(dataType: DataType, nullable: Boolean)

  /**
   * Check if we can generate efficient code for this data type.
   * Returns false for types that would require fallback to GpuRowToColumnConverter in codegen.
   * This allows fast-fail before attempting expensive code generation.
   */
  private def canSupportDirectCodegen(dataType: DataType): Boolean = {
    dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | DateType | 
           LongType | TimestampType | FloatType | DoubleType | StringType |
           BinaryType | _: DecimalType | NullType => true
      case ArrayType(elementType, _) => canSupportDirectCodegen(elementType)
      case st: StructType => st.fields.forall(f => canSupportDirectCodegen(f.dataType))
      case MapType(kt, vt, _) => canSupportDirectCodegen(kt) && canSupportDirectCodegen(vt)
      case CalendarIntervalType => true
      case _: YearMonthIntervalType => true
      case _: DayTimeIntervalType => true
      case _ => false
    }
  }

  /** Returns true iff we support this data type. */
  def canSupport(dataType: DataType): Boolean = UserDefinedType.sqlType(dataType) match {
    case NullType => true
    case _: AtomicType => true
    case _: CalendarIntervalType => true
    case t: StructType => t.forall(field => canSupport(field.dataType))
    case t: ArrayType if canSupport(t.elementType) => true
    case MapType(kt, vt, _) if canSupport(kt) && canSupport(vt) => true
    case _ => false
  }

  /**
   * Generate code to append a value directly to a RapidsHostColumnBuilder.
   * Generates direct append code without using converters.
   */
  private def generateBuilderAppendCode(
      ctx: CodegenContext,
      dataType: DataType,
      nullable: Boolean,
      valueVar: String,
      isNullVar: String,
      builderRef: String): String = {
    
    dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | DateType | 
           LongType | TimestampType | FloatType | DoubleType | 
           _: YearMonthIntervalType | _: DayTimeIntervalType =>
        if (nullable) {
          s"""
             |if ($isNullVar) {
             |  $builderRef.appendNull();
             |} else {
             |  $builderRef.append($valueVar);
             |}
           """.stripMargin
        } else {
          s"$builderRef.append($valueVar);"
        }
        
      case StringType =>
        if (nullable) {
          s"""
             |if ($isNullVar || $valueVar == null) {
             |  $builderRef.appendNull();
             |} else {
             |  $builderRef.appendUTF8String($valueVar.getBytes());
             |}
           """.stripMargin
        } else {
          s"$builderRef.appendUTF8String($valueVar.getBytes());"
        }
        
      case BinaryType =>
        if (nullable) {
          s"""
             |if ($isNullVar || $valueVar == null) {
             |  $builderRef.appendNull();
             |} else {
             |  $builderRef.appendByteList($valueVar);
             |}
           """.stripMargin
        } else {
          s"$builderRef.appendByteList($valueVar);"
        }
        
      case _: DecimalType =>
        if (nullable) {
          s"""
             |if ($isNullVar || $valueVar == null) {
             |  $builderRef.appendNull();
             |} else {
             |  $builderRef.append($valueVar.toJavaBigDecimal());
             |}
           """.stripMargin
        } else {
          s"$builderRef.append($valueVar.toJavaBigDecimal());"
        }
      
      case NullType =>
        s"$builderRef.appendNull();"
      
      case ArrayType(elementType, containsNull) =>
        generateArrayAppendCode(ctx, elementType, containsNull, nullable, valueVar, 
          isNullVar, builderRef)
        
      case structType: StructType =>
        generateStructAppendCode(ctx, structType, nullable, valueVar, isNullVar, builderRef)
        
      case MapType(keyType, valueType, valueContainsNull) =>
        generateMapAppendCode(ctx, keyType, valueType, valueContainsNull, nullable, 
          valueVar, isNullVar, builderRef)
      
      case CalendarIntervalType =>
        // CalendarIntervalType: appendStruct + append months/microseconds
        if (nullable) {
          s"""
             |if ($isNullVar || $valueVar == null) {
             |  $builderRef.appendNull();
             |} else {
             |  $builderRef.getChild(0).append($valueVar.months);
             |  $builderRef.getChild(1).append($valueVar.microseconds);
             |  $builderRef.endStruct();
             |}
           """.stripMargin
        } else {
          s"""
             |$builderRef.getChild(0).append($valueVar.months);
             |$builderRef.getChild(1).append($valueVar.microseconds);
             |$builderRef.endStruct();
           """.stripMargin
        }
        
      case _ =>
        // This should never be reached due to canSupportDirectCodegen check
        // But if it is, throw an exception that will trigger interpreted fallback
        throw new UnsupportedOperationException(
          s"Cannot generate direct append code for unsupported type: $dataType. " +
          s"Layer 2 check should have prevented this.")
    }
  }
  
  private def generateArrayAppendCode(
      ctx: CodegenContext,
      elementType: DataType,
      containsNull: Boolean,
      nullable: Boolean,
      valueVar: String,
      isNullVar: String,
      builderRef: String): String = {
    
    val arrayVar = ctx.freshName("array")
    val numElementsVar = ctx.freshName("numElements")
    val iVar = ctx.freshName("i")
    val childBuilderVar = ctx.freshName("childBuilder")
    val elementVar = ctx.freshName("element")
    val elementIsNullVar = ctx.freshName("elementIsNull")
    
    // Generate code to append each element
    // generateBuilderAppendCode now throws for unsupported types instead of returning null
    val elementAppendCode = generateBuilderAppendCode(ctx, elementType, containsNull,
      elementVar, elementIsNullVar, childBuilderVar)
    
    val loopBody = s"""
       |boolean $elementIsNullVar = $arrayVar.isNullAt($iVar);
       |${CodeGenerator.javaType(elementType)} $elementVar = 
       |  ${CodeGenerator.getValue(arrayVar, elementType, iVar)};
       |$elementAppendCode
     """.stripMargin
    
    val nullCheck = if (nullable) {
      s"""
         |if ($isNullVar || $valueVar == null) {
         |  $builderRef.appendNull();
         |} else {
       """.stripMargin
    } else {
      ""
    }
    
    val closeNullCheck = if (nullable) "}" else ""
    
    s"""
       |$nullCheck
       |  org.apache.spark.sql.catalyst.util.ArrayData $arrayVar = $valueVar;
       |  int $numElementsVar = $arrayVar.numElements();
       |  ${classOf[RapidsHostColumnBuilder].getName} $childBuilderVar = $builderRef.getChild(0);
       |  for (int $iVar = 0; $iVar < $numElementsVar; $iVar++) {
       |    $loopBody
       |  }
       |  $builderRef.endList();
       |$closeNullCheck
     """.stripMargin
  }
  
  private def generateStructAppendCode(
      ctx: CodegenContext,
      structType: StructType,
      nullable: Boolean,
      valueVar: String,
      isNullVar: String,
      builderRef: String): String = {
    
    val structVar = ctx.freshName("struct")
    
    // Generate code for each field
    // generateBuilderAppendCode now throws for unsupported types instead of returning null
    val fieldAppends = structType.fields.zipWithIndex.map { case (field, idx) =>
      val fieldBuilderVar = ctx.freshName(s"fieldBuilder$idx")
      val fieldValueVar = ctx.freshName(s"fieldValue$idx")
      val fieldIsNullVar = ctx.freshName(s"fieldIsNull$idx")
      
      val fieldAppendCode = generateBuilderAppendCode(ctx, field.dataType, field.nullable,
        fieldValueVar, fieldIsNullVar, fieldBuilderVar)
      
      s"""
         |${classOf[RapidsHostColumnBuilder].getName} $fieldBuilderVar = 
         |  $builderRef.getChild($idx);
         |boolean $fieldIsNullVar = $structVar.isNullAt($idx);
         |${CodeGenerator.javaType(field.dataType)} $fieldValueVar = 
         |  ${CodeGenerator.getValue(structVar, field.dataType, idx.toString)};
         |$fieldAppendCode
       """.stripMargin
    }.mkString("\n")
    
    val nullCheck = if (nullable) {
      s"""
         |if ($isNullVar || $valueVar == null) {
         |  $builderRef.appendNull();
         |} else {
       """.stripMargin
    } else {
      ""
    }
    
    val closeNullCheck = if (nullable) "}" else ""
    
    s"""
       |$nullCheck
       |  org.apache.spark.sql.catalyst.InternalRow $structVar = $valueVar;
       |  $fieldAppends
       |  $builderRef.endStruct();
       |$closeNullCheck
     """.stripMargin
  }
  
  private def generateMapAppendCode(
      ctx: CodegenContext,
      keyType: DataType,
      valueType: DataType,
      valueContainsNull: Boolean,
      nullable: Boolean,
      valueVar: String,
      isNullVar: String,
      builderRef: String): String = {
    
    val mapVar = ctx.freshName("map")
    val numElementsVar = ctx.freshName("numElements")
    val iVar = ctx.freshName("i")
    val keysVar = ctx.freshName("keys")
    val valuesVar = ctx.freshName("values")
    val structBuilderVar = ctx.freshName("structBuilder")
    val keyBuilderVar = ctx.freshName("keyBuilder")
    val valueBuilderVar = ctx.freshName("valueBuilder")
    val keyVar = ctx.freshName("key")
    val valueElemVar = ctx.freshName("valueElem")
    val keyIsNullVar = ctx.freshName("keyIsNull")
    val valueIsNullVar = ctx.freshName("valueIsNull")
    
    // Generate code to append key
    val keyAppendCode = generateBuilderAppendCode(ctx, keyType, false, // keys are never null
      keyVar, keyIsNullVar, keyBuilderVar)
    
    // Generate code to append value
    val valueAppendCode = generateBuilderAppendCode(ctx, valueType, valueContainsNull,
      valueElemVar, valueIsNullVar, valueBuilderVar)
    
    
    val loopBody = s"""
       |boolean $keyIsNullVar = $keysVar.isNullAt($iVar);
       |${CodeGenerator.javaType(keyType)} $keyVar = 
       |  ${CodeGenerator.getValue(keysVar, keyType, iVar)};
       |$keyAppendCode
       |boolean $valueIsNullVar = $valuesVar.isNullAt($iVar);
       |${CodeGenerator.javaType(valueType)} $valueElemVar = 
       |  ${CodeGenerator.getValue(valuesVar, valueType, iVar)};
       |$valueAppendCode
       |$structBuilderVar.endStruct();
     """.stripMargin
    
    val nullCheck = if (nullable) {
      s"""
         |if ($isNullVar || $valueVar == null) {
         |  $builderRef.appendNull();
         |} else {
       """.stripMargin
    } else {
      ""
    }
    
    val closeNullCheck = if (nullable) "}" else ""
    
    s"""
       |$nullCheck
       |  org.apache.spark.sql.catalyst.util.MapData $mapVar = $valueVar;
       |  int $numElementsVar = $mapVar.numElements();
       |  org.apache.spark.sql.catalyst.util.ArrayData $keysVar = $mapVar.keyArray();
       |  org.apache.spark.sql.catalyst.util.ArrayData $valuesVar = $mapVar.valueArray();
       |  ${classOf[RapidsHostColumnBuilder].getName} $structBuilderVar = $builderRef.getChild(0);
       |  ${classOf[RapidsHostColumnBuilder].getName} $keyBuilderVar = 
       |    $structBuilderVar.getChild(0);
       |  ${classOf[RapidsHostColumnBuilder].getName} $valueBuilderVar = 
       |    $structBuilderVar.getChild(1);
       |  for (int $iVar = 0; $iVar < $numElementsVar; $iVar++) {
       |    $loopBody
       |  }
       |  $builderRef.endList();
       |$closeNullCheck
     """.stripMargin
  }

  private def writeStructToBuffer(
      ctx: CodegenContext,
      input: String,
      index: String,
      schemas: Seq[Schema],
      rowWriter: String): String = {
    // Puts `input` in a local variable to avoid to re-evaluate it if it's a statement.
    val tmpInput = ctx.freshName("tmpInput")
    val fieldEvals = schemas.zipWithIndex.map { case (Schema(dt, nullable), i) =>
      val isNull = if (nullable) {
        JavaCode.isNullExpression(s"$tmpInput.isNullAt($i)")
      } else {
        FalseLiteral
      }
      ExprCode(isNull, JavaCode.expression(CodeGenerator.getValue(tmpInput, dt, i.toString), dt))
    }

    val rowWriterClass = classOf[UnsafeRowWriter].getName
    val structRowWriter = ctx.addMutableState(rowWriterClass, "rowWriter",
      v => s"$v = new $rowWriterClass($rowWriter, ${fieldEvals.length});")
    val previousCursor = ctx.freshName("previousCursor")
    s"""
       |final InternalRow $tmpInput = $input;
       |if ($tmpInput instanceof UnsafeRow) {
       |  $rowWriter.write($index, (UnsafeRow) $tmpInput);
       |} else {
       |  // Remember the current cursor so that we can calculate how many bytes are
       |  // written later.
       |  final int $previousCursor = $rowWriter.cursor();
       |  ${writeExpressionsToBuffer(ctx, tmpInput, fieldEvals, schemas, structRowWriter)}
       |  $rowWriter.setOffsetAndSizeFromPreviousCursor($index, $previousCursor);
       |}
     """.stripMargin
  }

  private def writeExpressionsToBuffer(
      ctx: CodegenContext,
      row: String,
      inputs: Seq[ExprCode],
      schemas: Seq[Schema],
      rowWriter: String,
      isTopLevel: Boolean = false): String = {
    val resetWriter = if (isTopLevel) {
      // For top level row writer, it always writes to the beginning of the global buffer holder,
      // which means its fixed-size region always in the same position, so we don't need to call
      // `reset` to set up its fixed-size region every time.
      if (inputs.map(_.isNull).forall(_ == FalseLiteral)) {
        // If all fields are not nullable, which means the null bits never changes, then we don't
        // need to clear it out every time.
        ""
      } else {
        s"$rowWriter.zeroOutNullBytes();"
      }
    } else {
      s"$rowWriter.resetRowWriter();"
    }

    val writeFields = inputs.zip(schemas).zipWithIndex.map {
      case ((input, Schema(dataType, nullable)), index) =>
        val dt = UserDefinedType.sqlType(dataType)

        val setNull = dt match {
          case t: DecimalType if t.precision > Decimal.MAX_LONG_DIGITS =>
            // Can't call setNullAt() for DecimalType with precision larger than 18.
            s"$rowWriter.write($index, (Decimal) null, ${t.precision}, ${t.scale});"
          case CalendarIntervalType => s"$rowWriter.write($index, (CalendarInterval) null);"
          case _ => s"$rowWriter.setNullAt($index);"
        }

        val writeField = writeElement(ctx, input.value, index.toString, dt, rowWriter)
        if (!nullable) {
          s"""
             |${input.code}
             |${writeField.trim}
           """.stripMargin
        } else {
          s"""
             |${input.code}
             |if (${input.isNull}) {
             |  ${setNull.trim}
             |} else {
             |  ${writeField.trim}
             |}
           """.stripMargin
        }
    }

    val writeFieldsCode = if (isTopLevel && (row == null || ctx.currentVars != null)) {
      // TODO: support whole stage codegen
      writeFields.mkString("\n")
    } else {
      assert(row != null, "the input row name cannot be null when generating code to write it.")
      ctx.splitExpressions(
        expressions = writeFields,
        funcName = "writeFields",
        arguments = Seq("InternalRow" -> row))
    }
    s"""
       |$resetWriter
       |$writeFieldsCode
     """.stripMargin
  }

  private def writeArrayToBuffer(
      ctx: CodegenContext,
      input: String,
      elementType: DataType,
      containsNull: Boolean,
      rowWriter: String): String = {
    // Puts `input` in a local variable to avoid to re-evaluate it if it's a statement.
    val tmpInput = ctx.freshName("tmpInput")
    val numElements = ctx.freshName("numElements")
    val index = ctx.freshName("index")

    val et = UserDefinedType.sqlType(elementType)

    val jt = CodeGenerator.javaType(et)

    val elementOrOffsetSize = et match {
      case t: DecimalType if t.precision <= Decimal.MAX_LONG_DIGITS => 8
      case _ if CodeGenerator.isPrimitiveType(jt) => et.defaultSize
      case _ => 8  // we need 8 bytes to store offset and length
    }

    val arrayWriterClass = classOf[UnsafeArrayWriter].getName
    val arrayWriter = ctx.addMutableState(arrayWriterClass, "arrayWriter",
      v => s"$v = new $arrayWriterClass($rowWriter, $elementOrOffsetSize);")

    val element = CodeGenerator.getValue(tmpInput, et, index)

    val elementAssignment = if (containsNull) {
      s"""
         |if ($tmpInput.isNullAt($index)) {
         |  $arrayWriter.setNull${elementOrOffsetSize}Bytes($index);
         |} else {
         |  ${writeElement(ctx, element, index, et, arrayWriter)}
         |}
       """.stripMargin
    } else {
      writeElement(ctx, element, index, et, arrayWriter)
    }

    s"""
       |final ArrayData $tmpInput = $input;
       |if ($tmpInput instanceof UnsafeArrayData) {
       |  $rowWriter.write((UnsafeArrayData) $tmpInput);
       |} else {
       |  final int $numElements = $tmpInput.numElements();
       |  $arrayWriter.initialize($numElements);
       |
       |  for (int $index = 0; $index < $numElements; $index++) {
       |    $elementAssignment
       |  }
       |}
     """.stripMargin
  }

  private def writeMapToBuffer(
      ctx: CodegenContext,
      input: String,
      index: String,
      keyType: DataType,
      valueType: DataType,
      valueContainsNull: Boolean,
      rowWriter: String): String = {
    // Puts `input` in a local variable to avoid to re-evaluate it if it's a statement.
    val tmpInput = ctx.freshName("tmpInput")
    val tmpCursor = ctx.freshName("tmpCursor")
    val previousCursor = ctx.freshName("previousCursor")

    // Writes out unsafe map according to the format described in `UnsafeMapData`.
    val keyArray = writeArrayToBuffer(
      ctx, s"$tmpInput.keyArray()", keyType, false, rowWriter)
    val valueArray = writeArrayToBuffer(
      ctx, s"$tmpInput.valueArray()", valueType, valueContainsNull, rowWriter)

    s"""
       |final MapData $tmpInput = $input;
       |if ($tmpInput instanceof UnsafeMapData) {
       |  $rowWriter.write($index, (UnsafeMapData) $tmpInput);
       |} else {
       |  // Remember the current cursor so that we can calculate how many bytes are
       |  // written later.
       |  final int $previousCursor = $rowWriter.cursor();
       |
       |  // preserve 8 bytes to write the key array numBytes later.
       |  $rowWriter.grow(8);
       |  $rowWriter.increaseCursor(8);
       |
       |  // Remember the current cursor so that we can write numBytes of key array later.
       |  final int $tmpCursor = $rowWriter.cursor();
       |
       |  $keyArray
       |
       |  // Write the numBytes of key array into the first 8 bytes.
       |  Platform.putLong(
       |    $rowWriter.getBuffer(),
       |    $tmpCursor - 8,
       |    $rowWriter.cursor() - $tmpCursor);
       |
       |  $valueArray
       |  $rowWriter.setOffsetAndSizeFromPreviousCursor($index, $previousCursor);
       |}
     """.stripMargin
  }

  private def writeElement(
      ctx: CodegenContext,
      input: String,
      index: String,
      dt: DataType,
      writer: String): String = dt match {
    case t: StructType =>
      writeStructToBuffer(
        ctx, input, index, t.map(e => Schema(e.dataType, e.nullable)), writer)

    case ArrayType(et, en) =>
      val previousCursor = ctx.freshName("previousCursor")
      s"""
         |// Remember the current cursor so that we can calculate how many bytes are
         |// written later.
         |final int $previousCursor = $writer.cursor();
         |${writeArrayToBuffer(ctx, input, et, en, writer)}
         |$writer.setOffsetAndSizeFromPreviousCursor($index, $previousCursor);
       """.stripMargin

    case MapType(kt, vt, vn) =>
      writeMapToBuffer(ctx, input, index, kt, vt, vn, writer)

    case DecimalType.Fixed(precision, scale) =>
      s"$writer.write($index, $input, $precision, $scale);"

    case NullType => ""

    case _ => s"$writer.write($index, $input);"
  }

  def createCode(
      ctx: CodegenContext,
      expressions: Seq[Expression],
      useSubexprElimination: Boolean = false): ExprCode = {
    val exprEvals = ctx.generateExpressions(expressions, useSubexprElimination)
    val exprSchemas = expressions.map(e => Schema(e.dataType, e.nullable))

    val numVarLenFields = exprSchemas.count {
      case Schema(dt, _) => !UnsafeRow.isFixedLength(dt)
      // TODO: consider large decimal and interval type
    }

    val rowWriterClass = classOf[UnsafeRowWriter].getName
    val rowWriter = ctx.addMutableState(rowWriterClass, "rowWriter",
      v => s"$v = new $rowWriterClass(${expressions.length}, ${numVarLenFields * 32});")

    // Evaluate all the subexpression.
    val evalSubexpr = ctx.subexprFunctionsCode

    val writeExpressions = writeExpressionsToBuffer(
      ctx, ctx.INPUT_ROW, exprEvals, exprSchemas, rowWriter, isTopLevel = true)

    val code =
      code"""
         |$rowWriter.reset();
         |$evalSubexpr
         |$writeExpressions
       """.stripMargin
    // `rowWriter` is declared as a class field, so we can access it directly in methods.
    ExprCode(code, FalseLiteral, JavaCode.expression(s"$rowWriter.getRow()", classOf[UnsafeRow]))
  }

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    bindReferences(in, inputSchema)

  def generate(
      expressions: Seq[Expression],
      subexpressionEliminationEnabled: Boolean): BridgeUnsafeProjection = {
    create(canonicalize(expressions), subexpressionEliminationEnabled)
  }

  protected def create(references: Seq[Expression]): BridgeUnsafeProjection = {
    create(references, subexpressionEliminationEnabled = false)
  }

  private def create(
      expressions: Seq[Expression],
      subexpressionEliminationEnabled: Boolean): BridgeUnsafeProjection = {
    // Check upfront if we can support all types with direct codegen
    // This provides a fast-fail mechanism before attempting expensive codegen
    val unsupportedTypes = expressions.map(_.dataType).filterNot(canSupportDirectCodegen)
    if (unsupportedTypes.nonEmpty) {
      throw new UnsupportedOperationException(
        s"Cannot generate code for types: ${unsupportedTypes.mkString(", ")}. " +
        "Falling back to interpreted evaluation.")
    }
    
    val ctx = newCodeGenContext()

    // Generate code to evaluate all expressions with subexpression elimination
    val exprEvals = ctx.generateExpressions(expressions, subexpressionEliminationEnabled)
    
    // Evaluate all subexpressions once
    val evalSubexpr = ctx.subexprFunctionsCode
    
    // Generate append code for each expression
    val appendCodes = expressions.zip(exprEvals).zipWithIndex.map { 
      case ((expr, exprEval), idx) =>
        // Generate builder append code with fallback to GpuRowToColumnConverter
        val directAppendCode = generateBuilderAppendCode(ctx,
          expr.dataType, expr.nullable, exprEval.value.toString, 
          exprEval.isNull.toString, s"builders[$idx]")
        
        s"""
           |${exprEval.code}
           |$directAppendCode
         """.stripMargin
    }
    
    val processRowBody = Seq(
      s"$evalSubexpr",
      appendCodes.mkString("\n")
    )
    
    // Split into multiple methods if needed to avoid 64KB method size limit
    val loopBody = ctx.splitExpressions(
      expressions = processRowBody,
      funcName = "processRow",
      arguments = Seq(
        "org.apache.spark.sql.catalyst.InternalRow" -> ctx.INPUT_ROW,
        s"${classOf[RapidsHostColumnBuilder].getName}[]" -> "builders"
      )
    )

    val codeBody =
      s"""
         |public java.lang.Object generate(Object[] references) {
         |  return new SpecificUnsafeProjection(references);
         |}
         |
         |class SpecificUnsafeProjection extends ${classOf[BridgeUnsafeProjection].getName} {
         |
         |  private Object[] references;
         |  ${ctx.declareMutableStates()}
         |
         |  public SpecificUnsafeProjection(Object[] references) {
         |    this.references = references;
         |    ${ctx.initMutableStates()}
         |  }
         |
         |  public void initialize(int partitionIndex) {
         |    ${ctx.initPartition()}
         |  }
         |
         |  public void apply(scala.collection.Iterator<InternalRow> rows,
         |    ${classOf[RapidsHostColumnBuilder].getName}[] builders) {
         |    while(rows.hasNext()) {
         |      InternalRow ${ctx.INPUT_ROW} = (InternalRow) rows.next();
         |      $loopBody
         |    }
         |  }
         |
         |  ${ctx.declareAddedFunctions()}
         |}
       """.stripMargin

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"code for ${expressions.mkString(",")}:\n${CodeFormatter.format(code)}")

    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[BridgeUnsafeProjection]
  }
}
