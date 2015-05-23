/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.parquet

import scala.collection.mutable.{Buffer, ArrayBuffer, HashMap}

import parquet.io.api.{PrimitiveConverter, GroupConverter, Binary, Converter}
import parquet.schema.MessageType

import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.expressions.{GenericRow, Row, Attribute}
import org.apache.spark.sql.parquet.CatalystConverter.FieldType

/**
 * Collection of converters of Parquet types (group and primitive types) that
 * model arrays and maps. The conversions are partly based on the AvroParquet
 * converters that are part of Parquet in order to be able to process these
 * types.
 *
 * There are several types of converters:
 * <ul>
 *   <li>[[org.apache.spark.sql.parquet.CatalystPrimitiveConverter]] for primitive
 *   (numeric, boolean and String) types</li>
 *   <li>[[org.apache.spark.sql.parquet.CatalystNativeArrayConverter]] for arrays
 *   of native JVM element types; note: currently null values are not supported!</li>
 *   <li>[[org.apache.spark.sql.parquet.CatalystArrayConverter]] for arrays of
 *   arbitrary element types (including nested element types); note: currently
 *   null values are not supported!</li>
 *   <li>[[org.apache.spark.sql.parquet.CatalystStructConverter]] for structs</li>
 *   <li>[[org.apache.spark.sql.parquet.CatalystMapConverter]] for maps; note:
 *   currently null values are not supported!</li>
 *   <li>[[org.apache.spark.sql.parquet.CatalystPrimitiveRowConverter]] for rows
 *   of only primitive element types</li>
 *   <li>[[org.apache.spark.sql.parquet.CatalystGroupConverter]] for other nested
 *   records, including the top-level row record</li>
 * </ul>
 */

private[sql] object CatalystConverter {
  // The type internally used for fields
  type FieldType = StructField

  // This is mostly Parquet convention (see, e.g., `ConversionPatterns`).
  // Note that "array" for the array elements is chosen by ParquetAvro.
  // Using a different value will result in Parquet silently dropping columns.
  val ARRAY_ELEMENTS_SCHEMA_NAME = "array"
  val MAP_KEY_SCHEMA_NAME = "key"
  val MAP_VALUE_SCHEMA_NAME = "value"
  val MAP_SCHEMA_NAME = "map"

  // TODO: consider using Array[T] for arrays to avoid boxing of primitive types
  type ArrayScalaType[T] = Seq[T]
  type StructScalaType[T] = Seq[T]
  type MapScalaType[K, V] = Map[K, V]

  protected[parquet] def createConverter(
      field: FieldType,
      fieldIndex: Int,
      parent: CatalystConverter): Converter = {
    val fieldType: DataType = field.dataType
    fieldType match {
      // For native JVM types we use a converter with native arrays
      case ArrayType(elementType: NativeType) => {
        new CatalystNativeArrayConverter(elementType, fieldIndex, parent)
      }
      // This is for other types of arrays, including those with nested fields
      case ArrayType(elementType: DataType) => {
        new CatalystArrayConverter(elementType, fieldIndex, parent)
      }
      case StructType(fields: Seq[StructField]) => {
        new CatalystStructConverter(fields.toArray, fieldIndex, parent)
      }
      case MapType(keyType: DataType, valueType: DataType) => {
        new CatalystMapConverter(
          Array(
            new FieldType(MAP_KEY_SCHEMA_NAME, keyType, false),
            new FieldType(MAP_VALUE_SCHEMA_NAME, valueType, true)),
          fieldIndex,
          parent)
      }
      // Strings, Shorts and Bytes do not have a corresponding type in Parquet
      // so we need to treat them separately
      case StringType => {
        new CatalystPrimitiveConverter(parent, fieldIndex) {
          override def addBinary(value: Binary): Unit =
            parent.updateString(fieldIndex, value)
        }
      }
      case ShortType => {
        new CatalystPrimitiveConverter(parent, fieldIndex) {
          override def addInt(value: Int): Unit =
            parent.updateShort(fieldIndex, value.asInstanceOf[ShortType.JvmType])
        }
      }
      case ByteType => {
        new CatalystPrimitiveConverter(parent, fieldIndex) {
          override def addInt(value: Int): Unit =
            parent.updateByte(fieldIndex, value.asInstanceOf[ByteType.JvmType])
        }
      }
      // All other primitive types use the default converter
      case ctype: NativeType => { // note: need the type tag here!
        new CatalystPrimitiveConverter(parent, fieldIndex)
      }
      case _ => throw new RuntimeException(
        s"unable to convert datatype ${field.dataType.toString} in CatalystConverter")
    }
  }

  protected[parquet] def createRootConverter(
      parquetSchema: MessageType,
      attributes: Seq[Attribute]): CatalystConverter = {
    // For non-nested types we use the optimized Row converter
    if (attributes.forall(a => ParquetTypesConverter.isPrimitiveType(a.dataType))) {
      new CatalystPrimitiveRowConverter(attributes.toArray)
    } else {
      new CatalystGroupConverter(attributes.toArray)
    }
  }
}

private[parquet] abstract class CatalystConverter extends GroupConverter {
  /**
   * The number of fields this group has
   */
  protected[parquet] val size: Int

  /**
   * The index of this converter in the parent
   */
  protected[parquet] val index: Int

  /**
   * The parent converter
   */
  protected[parquet] val parent: CatalystConverter

  /**
   * Called by child converters to update their value in its parent (this).
   * Note that if possible the more specific update methods below should be used
   * to avoid auto-boxing of native JVM types.
   *
   * @param fieldIndex
   * @param value
   */
  protected[parquet] def updateField(fieldIndex: Int, value: Any): Unit

  protected[parquet] def updateBoolean(fieldIndex: Int, value: Boolean): Unit =
    updateField(fieldIndex, value)

  protected[parquet] def updateInt(fieldIndex: Int, value: Int): Unit =
    updateField(fieldIndex, value)

  protected[parquet] def updateLong(fieldIndex: Int, value: Long): Unit =
    updateField(fieldIndex, value)

  protected[parquet] def updateShort(fieldIndex: Int, value: Short): Unit =
    updateField(fieldIndex, value)

  protected[parquet] def updateByte(fieldIndex: Int, value: Byte): Unit =
    updateField(fieldIndex, value)

  protected[parquet] def updateDouble(fieldIndex: Int, value: Double): Unit =
    updateField(fieldIndex, value)

  protected[parquet] def updateFloat(fieldIndex: Int, value: Float): Unit =
    updateField(fieldIndex, value)

  protected[parquet] def updateBinary(fieldIndex: Int, value: Binary): Unit =
    updateField(fieldIndex, value.getBytes)

  protected[parquet] def updateString(fieldIndex: Int, value: Binary): Unit =
    updateField(fieldIndex, value.toStringUsingUTF8)

  protected[parquet] def isRootConverter: Boolean = parent == null

  protected[parquet] def clearBuffer(): Unit

  /**
   * Should only be called in the root (group) converter!
   *
   * @return
   */
  def getCurrentRecord: Row = throw new UnsupportedOperationException
}

/**
 * A `parquet.io.api.GroupConverter` that is able to convert a Parquet record
 * to a [[org.apache.spark.sql.catalyst.expressions.Row]] object.
 *
 * @param schema The corresponding Catalyst schema in the form of a list of attributes.
 */
private[parquet] class CatalystGroupConverter(
    protected[parquet] val schema: Array[FieldType],
    protected[parquet] val index: Int,
    protected[parquet] val parent: CatalystConverter,
    protected[parquet] var current: ArrayBuffer[Any],
    protected[parquet] var buffer: ArrayBuffer[Row])
  extends CatalystConverter {

  def this(schema: Array[FieldType], index: Int, parent: CatalystConverter) =
    this(
      schema,
      index,
      parent,
      current=null,
      buffer=new ArrayBuffer[Row](
        CatalystArrayConverter.INITIAL_ARRAY_SIZE))

  /**
   * This constructor is used for the root converter only!
   */
  def this(attributes: Array[Attribute]) =
    this(attributes.map(a => new FieldType(a.name, a.dataType, a.nullable)), 0, null)

  protected [parquet] val converters: Array[Converter] =
    schema.map(field =>
      CatalystConverter.createConverter(field, schema.indexOf(field), this))
    .toArray

  override val size = schema.size

  override def getCurrentRecord: Row = {
    assert(isRootConverter, "getCurrentRecord should only be called in root group converter!")
    // TODO: use iterators if possible
    // Note: this will ever only be called in the root converter when the record has been
    // fully processed. Therefore it will be difficult to use mutable rows instead, since
    // any non-root converter never would be sure when it would be safe to re-use the buffer.
    new GenericRow(current.toArray)
  }

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

  // for child converters to update upstream values
  override protected[parquet] def updateField(fieldIndex: Int, value: Any): Unit = {
    current.update(fieldIndex, value)
  }

  override protected[parquet] def clearBuffer(): Unit = buffer.clear()

  override def start(): Unit = {
    current = ArrayBuffer.fill(size)(null)
    converters.foreach {
      converter => if (!converter.isPrimitive) {
        converter.asInstanceOf[CatalystConverter].clearBuffer
      }
    }
  }

  override def end(): Unit = {
    if (!isRootConverter) {
      assert(current!=null) // there should be no empty groups
      buffer.append(new GenericRow(current.toArray))
      parent.updateField(index, new GenericRow(buffer.toArray.asInstanceOf[Array[Any]]))
    }
  }
}

/**
 * A `parquet.io.api.GroupConverter` that is able to convert a Parquet record
 * to a [[org.apache.spark.sql.catalyst.expressions.Row]] object. Note that his
 * converter is optimized for rows of primitive types (non-nested records).
 */
private[parquet] class CatalystPrimitiveRowConverter(
    protected[parquet] val schema: Array[FieldType],
    protected[parquet] var current: ParquetRelation.RowType)
  extends CatalystConverter {

  // This constructor is used for the root converter only
  def this(attributes: Array[Attribute]) =
    this(
      attributes.map(a => new FieldType(a.name, a.dataType, a.nullable)),
      new ParquetRelation.RowType(attributes.length))

  protected [parquet] val converters: Array[Converter] =
    schema.map(field =>
      CatalystConverter.createConverter(field, schema.indexOf(field), this))
      .toArray

  override val size = schema.size

  override val index = 0

  override val parent = null

  // Should be only called in root group converter!
  override def getCurrentRecord: ParquetRelation.RowType = current

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

  // for child converters to update upstream values
  override protected[parquet] def updateField(fieldIndex: Int, value: Any): Unit = {
    throw new UnsupportedOperationException // child converters should use the
    // specific update methods below
  }

  override protected[parquet] def clearBuffer(): Unit = {}

  override def start(): Unit = {
    var i = 0
    while (i < size) {
      current.setNullAt(i)
      i = i + 1
    }
  }

  override def end(): Unit = {}

  // Overriden here to avoid auto-boxing for primitive types
  override protected[parquet] def updateBoolean(fieldIndex: Int, value: Boolean): Unit =
    current.setBoolean(fieldIndex, value)

  override protected[parquet] def updateInt(fieldIndex: Int, value: Int): Unit =
    current.setInt(fieldIndex, value)

  override protected[parquet] def updateLong(fieldIndex: Int, value: Long): Unit =
    current.setLong(fieldIndex, value)

  override protected[parquet] def updateShort(fieldIndex: Int, value: Short): Unit =
    current.setShort(fieldIndex, value)

  override protected[parquet] def updateByte(fieldIndex: Int, value: Byte): Unit =
    current.setByte(fieldIndex, value)

  override protected[parquet] def updateDouble(fieldIndex: Int, value: Double): Unit =
    current.setDouble(fieldIndex, value)

  override protected[parquet] def updateFloat(fieldIndex: Int, value: Float): Unit =
    current.setFloat(fieldIndex, value)

  override protected[parquet] def updateBinary(fieldIndex: Int, value: Binary): Unit =
    current.update(fieldIndex, value.getBytes)

  override protected[parquet] def updateString(fieldIndex: Int, value: Binary): Unit =
    current.setString(fieldIndex, value.toStringUsingUTF8)
}

/**
 * A `parquet.io.api.PrimitiveConverter` that converts Parquet types to Catalyst types.
 *
 * @param parent The parent group converter.
 * @param fieldIndex The index inside the record.
 */
private[parquet] class CatalystPrimitiveConverter(
    parent: CatalystConverter,
    fieldIndex: Int) extends PrimitiveConverter {
  override def addBinary(value: Binary): Unit =
    parent.updateBinary(fieldIndex, value)

  override def addBoolean(value: Boolean): Unit =
    parent.updateBoolean(fieldIndex, value)

  override def addDouble(value: Double): Unit =
    parent.updateDouble(fieldIndex, value)

  override def addFloat(value: Float): Unit =
    parent.updateFloat(fieldIndex, value)

  override def addInt(value: Int): Unit =
    parent.updateInt(fieldIndex, value)

  override def addLong(value: Long): Unit =
    parent.updateLong(fieldIndex, value)
}

object CatalystArrayConverter {
  val INITIAL_ARRAY_SIZE = 20
}

/**
 * A `parquet.io.api.GroupConverter` that converts a single-element groups that
 * match the characteristics of an array (see
 * [[org.apache.spark.sql.parquet.ParquetTypesConverter]]) into an
 * [[org.apache.spark.sql.catalyst.types.ArrayType]].
 *
 * @param elementType The type of the array elements (complex or primitive)
 * @param index The position of this (array) field inside its parent converter
 * @param parent The parent converter
 * @param buffer A data buffer
 */
private[parquet] class CatalystArrayConverter(
    val elementType: DataType,
    val index: Int,
    protected[parquet] val parent: CatalystConverter,
    protected[parquet] var buffer: Buffer[Any])
  extends CatalystConverter {

  def this(elementType: DataType, index: Int, parent: CatalystConverter) =
    this(
      elementType,
      index,
      parent,
      new ArrayBuffer[Any](CatalystArrayConverter.INITIAL_ARRAY_SIZE))

  protected[parquet] val converter: Converter = CatalystConverter.createConverter(
    new CatalystConverter.FieldType(
      CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME,
      elementType,
      false),
    fieldIndex=0,
    parent=this)

  override def getConverter(fieldIndex: Int): Converter = converter

  // arrays have only one (repeated) field, which is its elements
  override val size = 1

  override protected[parquet] def updateField(fieldIndex: Int, value: Any): Unit = {
    // fieldIndex is ignored (assumed to be zero but not checked)
    if(value == null) {
      throw new IllegalArgumentException("Null values inside Parquet arrays are not supported!")
    }
    buffer += value
  }

  override protected[parquet] def clearBuffer(): Unit = {
    buffer.clear()
  }

  override def start(): Unit = {
    if (!converter.isPrimitive) {
      converter.asInstanceOf[CatalystConverter].clearBuffer
    }
  }

  override def end(): Unit = {
    assert(parent != null)
    // here we need to make sure to use ArrayScalaType
    parent.updateField(index, buffer.toArray.toSeq)
    clearBuffer()
  }
}

/**
 * A `parquet.io.api.GroupConverter` that converts a single-element groups that
 * match the characteristics of an array (see
 * [[org.apache.spark.sql.parquet.ParquetTypesConverter]]) into an
 * [[org.apache.spark.sql.catalyst.types.ArrayType]].
 *
 * @param elementType The type of the array elements (native)
 * @param index The position of this (array) field inside its parent converter
 * @param parent The parent converter
 * @param capacity The (initial) capacity of the buffer
 */
private[parquet] class CatalystNativeArrayConverter(
    val elementType: NativeType,
    val index: Int,
    protected[parquet] val parent: CatalystConverter,
    protected[parquet] var capacity: Int = CatalystArrayConverter.INITIAL_ARRAY_SIZE)
  extends CatalystConverter {

  type NativeType = elementType.JvmType

  private var buffer: Array[NativeType] = elementType.classTag.newArray(capacity)

  private var elements: Int = 0

  protected[parquet] val converter: Converter = CatalystConverter.createConverter(
    new CatalystConverter.FieldType(
      CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME,
      elementType,
      false),
    fieldIndex=0,
    parent=this)

  override def getConverter(fieldIndex: Int): Converter = converter

  // arrays have only one (repeated) field, which is its elements
  override val size = 1

  override protected[parquet] def updateField(fieldIndex: Int, value: Any): Unit =
    throw new UnsupportedOperationException

  // Overriden here to avoid auto-boxing for primitive types
  override protected[parquet] def updateBoolean(fieldIndex: Int, value: Boolean): Unit = {
    checkGrowBuffer()
    buffer(elements) = value.asInstanceOf[NativeType]
    elements += 1
  }

  override protected[parquet] def updateInt(fieldIndex: Int, value: Int): Unit = {
    checkGrowBuffer()
    buffer(elements) = value.asInstanceOf[NativeType]
    elements += 1
  }

  override protected[parquet] def updateShort(fieldIndex: Int, value: Short): Unit = {
    checkGrowBuffer()
    buffer(elements) = value.asInstanceOf[NativeType]
    elements += 1
  }

  override protected[parquet] def updateByte(fieldIndex: Int, value: Byte): Unit = {
    checkGrowBuffer()
    buffer(elements) = value.asInstanceOf[NativeType]
    elements += 1
  }

  override protected[parquet] def updateLong(fieldIndex: Int, value: Long): Unit = {
    checkGrowBuffer()
    buffer(elements) = value.asInstanceOf[NativeType]
    elements += 1
  }

  override protected[parquet] def updateDouble(fieldIndex: Int, value: Double): Unit = {
    checkGrowBuffer()
    buffer(elements) = value.asInstanceOf[NativeType]
    elements += 1
  }

  override protected[parquet] def updateFloat(fieldIndex: Int, value: Float): Unit = {
    checkGrowBuffer()
    buffer(elements) = value.asInstanceOf[NativeType]
    elements += 1
  }

  override protected[parquet] def updateBinary(fieldIndex: Int, value: Binary): Unit = {
    checkGrowBuffer()
    buffer(elements) = value.getBytes.asInstanceOf[NativeType]
    elements += 1
  }

  override protected[parquet] def updateString(fieldIndex: Int, value: Binary): Unit = {
    checkGrowBuffer()
    buffer(elements) = value.toStringUsingUTF8.asInstanceOf[NativeType]
    elements += 1
  }

  override protected[parquet] def clearBuffer(): Unit = {
    elements = 0
  }

  override def start(): Unit = {}

  override def end(): Unit = {
    assert(parent != null)
    // here we need to make sure to use ArrayScalaType
    parent.updateField(
      index,
      buffer.slice(0, elements).toSeq)
    clearBuffer()
  }

  private def checkGrowBuffer(): Unit = {
    if (elements >= capacity) {
      val newCapacity = 2 * capacity
      val tmp: Array[NativeType] = elementType.classTag.newArray(newCapacity)
      Array.copy(buffer, 0, tmp, 0, capacity)
      buffer = tmp
      capacity = newCapacity
    }
  }
}

/**
 * This converter is for multi-element groups of primitive or complex types
 * that have repetition level optional or required (so struct fields).
 *
 * @param schema The corresponding Catalyst schema in the form of a list of
 *               attributes.
 * @param index
 * @param parent
 */
private[parquet] class CatalystStructConverter(
    override protected[parquet] val schema: Array[FieldType],
    override protected[parquet] val index: Int,
    override protected[parquet] val parent: CatalystConverter)
  extends CatalystGroupConverter(schema, index, parent) {

  override protected[parquet] def clearBuffer(): Unit = {}

  // TODO: think about reusing the buffer
  override def end(): Unit = {
    assert(!isRootConverter)
    // here we need to make sure to use StructScalaType
    // Note: we need to actually make a copy of the array since we
    // may be in a nested field
    parent.updateField(index, new GenericRow(current.toArray))
  }
}

/**
 * A `parquet.io.api.GroupConverter` that converts two-element groups that
 * match the characteristics of a map (see
 * [[org.apache.spark.sql.parquet.ParquetTypesConverter]]) into an
 * [[org.apache.spark.sql.catalyst.types.MapType]].
 *
 * @param schema
 * @param index
 * @param parent
 */
private[parquet] class CatalystMapConverter(
    protected[parquet] val schema: Array[FieldType],
    override protected[parquet] val index: Int,
    override protected[parquet] val parent: CatalystConverter)
  extends CatalystConverter {

  private val map = new HashMap[Any, Any]()

  private val keyValueConverter = new CatalystConverter {
    private var currentKey: Any = null
    private var currentValue: Any = null
    val keyConverter = CatalystConverter.createConverter(schema(0), 0, this)
    val valueConverter = CatalystConverter.createConverter(schema(1), 1, this)

    override def getConverter(fieldIndex: Int): Converter = {
      if (fieldIndex == 0) keyConverter else valueConverter
    }

    override def end(): Unit = CatalystMapConverter.this.map += currentKey -> currentValue

    override def start(): Unit = {
      currentKey = null
      currentValue = null
    }

    override protected[parquet] val size: Int = 2
    override protected[parquet] val index: Int = 0
    override protected[parquet] val parent: CatalystConverter = CatalystMapConverter.this

    override protected[parquet] def updateField(fieldIndex: Int, value: Any): Unit = {
      fieldIndex match {
        case 0 =>
          currentKey = value
        case 1 =>
          currentValue = value
        case _ =>
          new RuntimePermission(s"trying to update Map with fieldIndex $fieldIndex")
      }
    }

    override protected[parquet] def clearBuffer(): Unit = {}
  }

  override protected[parquet] val size: Int = 1

  override protected[parquet] def clearBuffer(): Unit = {}

  override def start(): Unit = {
    map.clear()
  }

  override def end(): Unit = {
    // here we need to make sure to use MapScalaType
    parent.updateField(index, map.toMap)
  }

  override def getConverter(fieldIndex: Int): Converter = keyValueConverter

  override protected[parquet] def updateField(fieldIndex: Int, value: Any): Unit =
    throw new UnsupportedOperationException
}
