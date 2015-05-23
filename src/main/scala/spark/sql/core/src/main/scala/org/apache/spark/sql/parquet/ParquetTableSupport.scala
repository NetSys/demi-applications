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

import org.apache.hadoop.conf.Configuration

import parquet.column.ParquetProperties
import parquet.hadoop.ParquetOutputFormat
import parquet.hadoop.api.ReadSupport.ReadContext
import parquet.hadoop.api.{ReadSupport, WriteSupport}
import parquet.io.api._
import parquet.schema.{MessageType, MessageTypeParser}

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, Row}
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.execution.SparkSqlSerializer
import com.google.common.io.BaseEncoding

/**
 * A `parquet.io.api.RecordMaterializer` for Rows.
 *
 *@param root The root group converter for the record.
 */
private[parquet] class RowRecordMaterializer(root: CatalystConverter)
  extends RecordMaterializer[Row] {

  def this(parquetSchema: MessageType, attributes: Seq[Attribute]) =
    this(CatalystConverter.createRootConverter(parquetSchema, attributes))

  override def getCurrentRecord: Row = root.getCurrentRecord

  override def getRootConverter: GroupConverter = root.asInstanceOf[GroupConverter]
}

/**
 * A `parquet.hadoop.api.ReadSupport` for Row objects.
 */
private[parquet] class RowReadSupport extends ReadSupport[Row] with Logging {

  override def prepareForRead(
      conf: Configuration,
      stringMap: java.util.Map[String, String],
      fileSchema: MessageType,
      readContext: ReadContext): RecordMaterializer[Row] = {
    log.debug(s"preparing for read with Parquet file schema $fileSchema")
    // Note: this very much imitates AvroParquet
    val parquetSchema = readContext.getRequestedSchema
    var schema: Seq[Attribute] = null

    if (readContext.getReadSupportMetadata != null) {
      // first try to find the read schema inside the metadata (can result from projections)
      if (
        readContext
          .getReadSupportMetadata
          .get(RowReadSupport.SPARK_ROW_REQUESTED_SCHEMA) != null) {
        schema = ParquetTypesConverter.convertFromString(
          readContext.getReadSupportMetadata.get(RowReadSupport.SPARK_ROW_REQUESTED_SCHEMA))
      } else {
        // if unavailable, try the schema that was read originally from the file or provided
        // during the creation of the Parquet relation
        if (readContext.getReadSupportMetadata.get(RowReadSupport.SPARK_METADATA_KEY) != null) {
          schema = ParquetTypesConverter.convertFromString(
            readContext.getReadSupportMetadata.get(RowReadSupport.SPARK_METADATA_KEY))
        }
      }
    }
    // if both unavailable, fall back to deducing the schema from the given Parquet schema
    if (schema == null)  {
      log.debug("falling back to Parquet read schema")
      schema = ParquetTypesConverter.convertToAttributes(parquetSchema)
    }
    log.debug(s"list of attributes that will be read: $schema")
    new RowRecordMaterializer(parquetSchema, schema)
  }

  override def init(
      configuration: Configuration,
      keyValueMetaData: java.util.Map[String, String],
      fileSchema: MessageType): ReadContext = {
    var parquetSchema: MessageType = fileSchema
    var metadata: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    val requestedAttributes = RowReadSupport.getRequestedSchema(configuration)

    if (requestedAttributes != null) {
      parquetSchema = ParquetTypesConverter.convertFromAttributes(requestedAttributes)
      metadata.put(
        RowReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
        ParquetTypesConverter.convertToString(requestedAttributes))
    }

    val origAttributesStr: String = configuration.get(RowWriteSupport.SPARK_ROW_SCHEMA)
    if (origAttributesStr != null) {
      metadata.put(RowReadSupport.SPARK_METADATA_KEY, origAttributesStr)
    }

    return new ReadSupport.ReadContext(parquetSchema, metadata)
  }
}

private[parquet] object RowReadSupport {
  val SPARK_ROW_REQUESTED_SCHEMA = "org.apache.spark.sql.parquet.row.requested_schema"
  val SPARK_METADATA_KEY = "org.apache.spark.sql.parquet.row.metadata"

  private def getRequestedSchema(configuration: Configuration): Seq[Attribute] = {
    val schemaString = configuration.get(RowReadSupport.SPARK_ROW_REQUESTED_SCHEMA)
    if (schemaString == null) null else ParquetTypesConverter.convertFromString(schemaString)
  }
}

/**
 * A `parquet.hadoop.api.WriteSupport` for Row ojects.
 */
private[parquet] class RowWriteSupport extends WriteSupport[Row] with Logging {

  private[parquet] var writer: RecordConsumer = null
  private[parquet] var attributes: Seq[Attribute] = null

  override def init(configuration: Configuration): WriteSupport.WriteContext = {
    attributes = if (attributes == null) RowWriteSupport.getSchema(configuration) else attributes
    
    log.debug(s"write support initialized for requested schema $attributes")
    ParquetRelation.enableLogForwarding()
    new WriteSupport.WriteContext(
      ParquetTypesConverter.convertFromAttributes(attributes),
      new java.util.HashMap[java.lang.String, java.lang.String]())
  }

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
    writer = recordConsumer
    log.debug(s"preparing for write with schema $attributes")
  }

  override def write(record: Row): Unit = {
    if (attributes.size > record.size) {
      throw new IndexOutOfBoundsException(
        s"Trying to write more fields than contained in row (${attributes.size}>${record.size})")
    }

    var index = 0
    writer.startMessage()
    while(index < attributes.size) {
      // null values indicate optional fields but we do not check currently
      if (record(index) != null && record(index) != Nil) {
        writer.startField(attributes(index).name, index)
        writeValue(attributes(index).dataType, record(index))
        writer.endField(attributes(index).name, index)
      }
      index = index + 1
    }
    writer.endMessage()
  }

  private[parquet] def writeValue(schema: DataType, value: Any): Unit = {
    if (value != null && value != Nil) {
      schema match {
        case t @ ArrayType(_) => writeArray(
          t,
          value.asInstanceOf[CatalystConverter.ArrayScalaType[_]])
        case t @ MapType(_, _) => writeMap(
          t,
          value.asInstanceOf[CatalystConverter.MapScalaType[_, _]])
        case t @ StructType(_) => writeStruct(
          t,
          value.asInstanceOf[CatalystConverter.StructScalaType[_]])
        case _ => writePrimitive(schema.asInstanceOf[PrimitiveType], value)
      }
    }
  }

  private[parquet] def writePrimitive(schema: PrimitiveType, value: Any): Unit = {
    if (value != null && value != Nil) {
      schema match {
        case StringType => writer.addBinary(
          Binary.fromByteArray(
            value.asInstanceOf[String].getBytes("utf-8")
          )
        )
        case IntegerType => writer.addInteger(value.asInstanceOf[Int])
        case ShortType => writer.addInteger(value.asInstanceOf[Short])
        case LongType => writer.addLong(value.asInstanceOf[Long])
        case ByteType => writer.addInteger(value.asInstanceOf[Byte])
        case DoubleType => writer.addDouble(value.asInstanceOf[Double])
        case FloatType => writer.addFloat(value.asInstanceOf[Float])
        case BooleanType => writer.addBoolean(value.asInstanceOf[Boolean])
        case _ => sys.error(s"Do not know how to writer $schema to consumer")
      }
    }
  }

  private[parquet] def writeStruct(
      schema: StructType,
      struct: CatalystConverter.StructScalaType[_]): Unit = {
    if (struct != null && struct != Nil) {
      val fields = schema.fields.toArray
      writer.startGroup()
      var i = 0
      while(i < fields.size) {
        if (struct(i) != null && struct(i) != Nil) {
          writer.startField(fields(i).name, i)
          writeValue(fields(i).dataType, struct(i))
          writer.endField(fields(i).name, i)
        }
        i = i + 1
      }
      writer.endGroup()
    }
  }

  // TODO: support null values, see
  // https://issues.apache.org/jira/browse/SPARK-1649
  private[parquet] def writeArray(
      schema: ArrayType,
      array: CatalystConverter.ArrayScalaType[_]): Unit = {
    val elementType = schema.elementType
    writer.startGroup()
    if (array.size > 0) {
      writer.startField(CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME, 0)
      var i = 0
      while(i < array.size) {
        writeValue(elementType, array(i))
        i = i + 1
      }
      writer.endField(CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME, 0)
    }
    writer.endGroup()
  }

  // TODO: support null values, see
  // https://issues.apache.org/jira/browse/SPARK-1649
  private[parquet] def writeMap(
      schema: MapType,
      map: CatalystConverter.MapScalaType[_, _]): Unit = {
    writer.startGroup()
    if (map.size > 0) {
      writer.startField(CatalystConverter.MAP_SCHEMA_NAME, 0)
      writer.startGroup()
      writer.startField(CatalystConverter.MAP_KEY_SCHEMA_NAME, 0)
      for(key <- map.keys) {
        writeValue(schema.keyType, key)
      }
      writer.endField(CatalystConverter.MAP_KEY_SCHEMA_NAME, 0)
      writer.startField(CatalystConverter.MAP_VALUE_SCHEMA_NAME, 1)
      for(value <- map.values) {
        writeValue(schema.valueType, value)
      }
      writer.endField(CatalystConverter.MAP_VALUE_SCHEMA_NAME, 1)
      writer.endGroup()
      writer.endField(CatalystConverter.MAP_SCHEMA_NAME, 0)
    }
    writer.endGroup()
  }
}

// Optimized for non-nested rows
private[parquet] class MutableRowWriteSupport extends RowWriteSupport {
  override def write(record: Row): Unit = {
    if (attributes.size > record.size) {
      throw new IndexOutOfBoundsException(
        s"Trying to write more fields than contained in row (${attributes.size}>${record.size})")
    }

    var index = 0
    writer.startMessage()
    while(index < attributes.size) {
      // null values indicate optional fields but we do not check currently
      if (record(index) != null && record(index) != Nil) {
        writer.startField(attributes(index).name, index)
        consumeType(attributes(index).dataType, record, index)
        writer.endField(attributes(index).name, index)
      }
      index = index + 1
    }
    writer.endMessage()
  }

  private def consumeType(
      ctype: DataType,
      record: Row,
      index: Int): Unit = {
    ctype match {
      case StringType => writer.addBinary(
        Binary.fromByteArray(
          record(index).asInstanceOf[String].getBytes("utf-8")
        )
      )
      case IntegerType => writer.addInteger(record.getInt(index))
      case ShortType => writer.addInteger(record.getShort(index))
      case LongType => writer.addLong(record.getLong(index))
      case ByteType => writer.addInteger(record.getByte(index))
      case DoubleType => writer.addDouble(record.getDouble(index))
      case FloatType => writer.addFloat(record.getFloat(index))
      case BooleanType => writer.addBoolean(record.getBoolean(index))
      case _ => sys.error(s"Unsupported datatype $ctype, cannot write to consumer")
    }
  }
}

private[parquet] object RowWriteSupport {
  val SPARK_ROW_SCHEMA: String = "org.apache.spark.sql.parquet.row.attributes"

  def getSchema(configuration: Configuration): Seq[Attribute] = {
    val schemaString = configuration.get(RowWriteSupport.SPARK_ROW_SCHEMA)
    if (schemaString == null) {
      throw new RuntimeException("Missing schema!")
    }
    ParquetTypesConverter.convertFromString(schemaString)
  }

  def setSchema(schema: Seq[Attribute], configuration: Configuration) {
    val encoded = ParquetTypesConverter.convertToString(schema)
    configuration.set(SPARK_ROW_SCHEMA, encoded)
    configuration.set(
      ParquetOutputFormat.WRITER_VERSION,
      ParquetProperties.WriterVersion.PARQUET_1_0.toString)
  }
}

