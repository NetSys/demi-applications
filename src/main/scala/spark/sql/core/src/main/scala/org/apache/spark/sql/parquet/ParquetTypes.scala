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

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job

import parquet.hadoop.{ParquetFileReader, Footer, ParquetFileWriter}
import parquet.hadoop.metadata.{ParquetMetadata, FileMetaData}
import parquet.hadoop.util.ContextUtil
import parquet.schema.{Type => ParquetType, PrimitiveType => ParquetPrimitiveType, MessageType}
import parquet.schema.{GroupType => ParquetGroupType, OriginalType => ParquetOriginalType, ConversionPatterns}
import parquet.schema.PrimitiveType.{PrimitiveTypeName => ParquetPrimitiveTypeName}
import parquet.schema.Type.Repetition

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Attribute}
import org.apache.spark.sql.catalyst.types._

// Implicits
import scala.collection.JavaConversions._

private[parquet] object ParquetTypesConverter extends Logging {
  def isPrimitiveType(ctype: DataType): Boolean =
    classOf[PrimitiveType] isAssignableFrom ctype.getClass

  def toPrimitiveDataType(parquetType : ParquetPrimitiveTypeName): DataType = parquetType match {
    case ParquetPrimitiveTypeName.BINARY => StringType
    case ParquetPrimitiveTypeName.BOOLEAN => BooleanType
    case ParquetPrimitiveTypeName.DOUBLE => DoubleType
    case ParquetPrimitiveTypeName.FIXED_LEN_BYTE_ARRAY => ArrayType(ByteType)
    case ParquetPrimitiveTypeName.FLOAT => FloatType
    case ParquetPrimitiveTypeName.INT32 => IntegerType
    case ParquetPrimitiveTypeName.INT64 => LongType
    case ParquetPrimitiveTypeName.INT96 =>
      // TODO: add BigInteger type? TODO(andre) use DecimalType instead????
      sys.error("Potential loss of precision: cannot convert INT96")
    case _ => sys.error(
      s"Unsupported parquet datatype $parquetType")
  }

  /**
   * Converts a given Parquet `Type` into the corresponding
   * [[org.apache.spark.sql.catalyst.types.DataType]].
   *
   * We apply the following conversion rules:
   * <ul>
   *   <li> Primitive types are converter to the corresponding primitive type.</li>
   *   <li> Group types that have a single field that is itself a group, which has repetition
   *        level `REPEATED`, are treated as follows:<ul>
   *          <li> If the nested group has name `values`, the surrounding group is converted
   *               into an [[ArrayType]] with the corresponding field type (primitive or
   *               complex) as element type.</li>
   *          <li> If the nested group has name `map` and two fields (named `key` and `value`),
   *               the surrounding group is converted into a [[MapType]]
   *               with the corresponding key and value (value possibly complex) types.
   *               Note that we currently assume map values are not nullable.</li>
   *   <li> Other group types are converted into a [[StructType]] with the corresponding
   *        field types.</li></ul></li>
   * </ul>
   * Note that fields are determined to be `nullable` if and only if their Parquet repetition
   * level is not `REQUIRED`.
   *
   * @param parquetType The type to convert.
   * @return The corresponding Catalyst type.
   */
  def toDataType(parquetType: ParquetType): DataType = {
    def correspondsToMap(groupType: ParquetGroupType): Boolean = {
      if (groupType.getFieldCount != 1 || groupType.getFields.apply(0).isPrimitive) {
        false
      } else {
        // This mostly follows the convention in ``parquet.schema.ConversionPatterns``
        val keyValueGroup = groupType.getFields.apply(0).asGroupType()
        keyValueGroup.getRepetition == Repetition.REPEATED &&
          keyValueGroup.getName == CatalystConverter.MAP_SCHEMA_NAME &&
          keyValueGroup.getFieldCount == 2 &&
          keyValueGroup.getFields.apply(0).getName == CatalystConverter.MAP_KEY_SCHEMA_NAME &&
          keyValueGroup.getFields.apply(1).getName == CatalystConverter.MAP_VALUE_SCHEMA_NAME
      }
    }

    def correspondsToArray(groupType: ParquetGroupType): Boolean = {
      groupType.getFieldCount == 1 &&
        groupType.getFieldName(0) == CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME &&
        groupType.getFields.apply(0).getRepetition == Repetition.REPEATED
    }

    if (parquetType.isPrimitive) {
      toPrimitiveDataType(parquetType.asPrimitiveType.getPrimitiveTypeName)
    } else {
      val groupType = parquetType.asGroupType()
      parquetType.getOriginalType match {
        // if the schema was constructed programmatically there may be hints how to convert
        // it inside the metadata via the OriginalType field
        case ParquetOriginalType.LIST => { // TODO: check enums!
          assert(groupType.getFieldCount == 1)
          val field = groupType.getFields.apply(0)
          new ArrayType(toDataType(field))
        }
        case ParquetOriginalType.MAP => {
          assert(
            !groupType.getFields.apply(0).isPrimitive,
            "Parquet Map type malformatted: expected nested group for map!")
          val keyValueGroup = groupType.getFields.apply(0).asGroupType()
          assert(
            keyValueGroup.getFieldCount == 2,
            "Parquet Map type malformatted: nested group should have 2 (key, value) fields!")
          val keyType = toDataType(keyValueGroup.getFields.apply(0))
          assert(keyValueGroup.getFields.apply(0).getRepetition == Repetition.REQUIRED)
          val valueType = toDataType(keyValueGroup.getFields.apply(1))
          assert(keyValueGroup.getFields.apply(1).getRepetition == Repetition.REQUIRED)
          new MapType(keyType, valueType)
        }
        case _ => {
          // Note: the order of these checks is important!
          if (correspondsToMap(groupType)) { // MapType
            val keyValueGroup = groupType.getFields.apply(0).asGroupType()
            val keyType = toDataType(keyValueGroup.getFields.apply(0))
            assert(keyValueGroup.getFields.apply(0).getRepetition == Repetition.REQUIRED)
            val valueType = toDataType(keyValueGroup.getFields.apply(1))
            assert(keyValueGroup.getFields.apply(1).getRepetition == Repetition.REQUIRED)
            new MapType(keyType, valueType)
          } else if (correspondsToArray(groupType)) { // ArrayType
            val elementType = toDataType(groupType.getFields.apply(0))
            new ArrayType(elementType)
          } else { // everything else: StructType
            val fields = groupType
              .getFields
              .map(ptype => new StructField(
              ptype.getName,
              toDataType(ptype),
              ptype.getRepetition != Repetition.REQUIRED))
            new StructType(fields)
          }
        }
      }
    }
  }

  /**
   * For a given Catalyst [[org.apache.spark.sql.catalyst.types.DataType]] return
   * the name of the corresponding Parquet primitive type or None if the given type
   * is not primitive.
   *
   * @param ctype The type to convert
   * @return The name of the corresponding Parquet primitive type
   */
  def fromPrimitiveDataType(ctype: DataType):
      Option[ParquetPrimitiveTypeName] = ctype match {
    case StringType => Some(ParquetPrimitiveTypeName.BINARY)
    case BooleanType => Some(ParquetPrimitiveTypeName.BOOLEAN)
    case DoubleType => Some(ParquetPrimitiveTypeName.DOUBLE)
    case ArrayType(ByteType) =>
      Some(ParquetPrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
    case FloatType => Some(ParquetPrimitiveTypeName.FLOAT)
    case IntegerType => Some(ParquetPrimitiveTypeName.INT32)
    // There is no type for Byte or Short so we promote them to INT32.
    case ShortType => Some(ParquetPrimitiveTypeName.INT32)
    case ByteType => Some(ParquetPrimitiveTypeName.INT32)
    case LongType => Some(ParquetPrimitiveTypeName.INT64)
    case _ => None
  }

  /**
   * Converts a given Catalyst [[org.apache.spark.sql.catalyst.types.DataType]] into
   * the corresponding Parquet `Type`.
   *
   * The conversion follows the rules below:
   * <ul>
   *   <li> Primitive types are converted into Parquet's primitive types.</li>
   *   <li> [[org.apache.spark.sql.catalyst.types.StructType]]s are converted
   *        into Parquet's `GroupType` with the corresponding field types.</li>
   *   <li> [[org.apache.spark.sql.catalyst.types.ArrayType]]s are converted
   *        into a 2-level nested group, where the outer group has the inner
   *        group as sole field. The inner group has name `values` and
   *        repetition level `REPEATED` and has the element type of
   *        the array as schema. We use Parquet's `ConversionPatterns` for this
   *        purpose.</li>
   *   <li> [[org.apache.spark.sql.catalyst.types.MapType]]s are converted
   *        into a nested (2-level) Parquet `GroupType` with two fields: a key
   *        type and a value type. The nested group has repetition level
   *        `REPEATED` and name `map`. We use Parquet's `ConversionPatterns`
   *        for this purpose</li>
   * </ul>
   * Parquet's repetition level is generally set according to the following rule:
   * <ul>
   *   <li> If the call to `fromDataType` is recursive inside an enclosing `ArrayType` or
   *   `MapType`, then the repetition level is set to `REPEATED`.</li>
   *   <li> Otherwise, if the attribute whose type is converted is `nullable`, the Parquet
   *   type gets repetition level `OPTIONAL` and otherwise `REQUIRED`.</li>
   * </ul>
   *
   *@param ctype The type to convert
   * @param name The name of the [[org.apache.spark.sql.catalyst.expressions.Attribute]]
   *             whose type is converted
   * @param nullable When true indicates that the attribute is nullable
   * @param inArray When true indicates that this is a nested attribute inside an array.
   * @return The corresponding Parquet type.
   */
  def fromDataType(
      ctype: DataType,
      name: String,
      nullable: Boolean = true,
      inArray: Boolean = false): ParquetType = {
    val repetition =
      if (inArray) {
        Repetition.REPEATED
      } else {
        if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
      }
    val primitiveType = fromPrimitiveDataType(ctype)
    if (primitiveType.isDefined) {
      new ParquetPrimitiveType(repetition, primitiveType.get, name)
    } else {
      ctype match {
        case ArrayType(elementType) => {
          val parquetElementType = fromDataType(
            elementType,
            CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME,
            nullable = false,
            inArray = true)
            ConversionPatterns.listType(repetition, name, parquetElementType)
        }
        case StructType(structFields) => {
          val fields = structFields.map {
            field => fromDataType(field.dataType, field.name, field.nullable, inArray = false)
          }
          new ParquetGroupType(repetition, name, fields)
        }
        case MapType(keyType, valueType) => {
          val parquetKeyType =
            fromDataType(
              keyType,
              CatalystConverter.MAP_KEY_SCHEMA_NAME,
              nullable = false,
              inArray = false)
          val parquetValueType =
            fromDataType(
              valueType,
              CatalystConverter.MAP_VALUE_SCHEMA_NAME,
              nullable = false,
              inArray = false)
          ConversionPatterns.mapType(
            repetition,
            name,
            parquetKeyType,
            parquetValueType)
        }
        case _ => sys.error(s"Unsupported datatype $ctype")
      }
    }
  }

  def convertToAttributes(parquetSchema: ParquetType): Seq[Attribute] = {
    parquetSchema
      .asGroupType()
      .getFields
      .map(
        field =>
          new AttributeReference(
            field.getName,
            toDataType(field),
            field.getRepetition != Repetition.REQUIRED)())
  }

  def convertFromAttributes(attributes: Seq[Attribute]): MessageType = {
    val fields = attributes.map(
      attribute =>
        fromDataType(attribute.dataType, attribute.name, attribute.nullable))
    new MessageType("root", fields)
  }

  def convertFromString(string: String): Seq[Attribute] = {
    DataType(string) match {
      case s: StructType => s.toAttributes
      case other => sys.error(s"Can convert $string to row")
    }
  }

  def convertToString(schema: Seq[Attribute]): String = {
    StructType.fromAttributes(schema).toString
  }

  def writeMetaData(attributes: Seq[Attribute], origPath: Path, conf: Configuration): Unit = {
    if (origPath == null) {
      throw new IllegalArgumentException("Unable to write Parquet metadata: path is null")
    }
    val fs = origPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException(
        s"Unable to write Parquet metadata: path $origPath is incorrectly formatted")
    }
    val path = origPath.makeQualified(fs)
    if (fs.exists(path) && !fs.getFileStatus(path).isDir) {
      throw new IllegalArgumentException(s"Expected to write to directory $path but found file")
    }
    val metadataPath = new Path(path, ParquetFileWriter.PARQUET_METADATA_FILE)
    if (fs.exists(metadataPath)) {
      try {
        fs.delete(metadataPath, true)
      } catch {
        case e: IOException =>
          throw new IOException(s"Unable to delete previous PARQUET_METADATA_FILE at $metadataPath")
      }
    }
    val extraMetadata = new java.util.HashMap[String, String]()
    extraMetadata.put(
      RowReadSupport.SPARK_METADATA_KEY,
      ParquetTypesConverter.convertToString(attributes))
    // TODO: add extra data, e.g., table name, date, etc.?

    val parquetSchema: MessageType =
      ParquetTypesConverter.convertFromAttributes(attributes)
    val metaData: FileMetaData = new FileMetaData(
      parquetSchema,
      extraMetadata,
      "Spark")

    ParquetRelation.enableLogForwarding()
    ParquetFileWriter.writeMetadataFile(
      conf,
      path,
      new Footer(path, new ParquetMetadata(metaData, Nil)) :: Nil)
  }

  /**
   * Try to read Parquet metadata at the given Path. We first see if there is a summary file
   * in the parent directory. If so, this is used. Else we read the actual footer at the given
   * location.
   * @param origPath The path at which we expect one (or more) Parquet files.
   * @param configuration The Hadoop configuration to use.
   * @return The `ParquetMetadata` containing among other things the schema.
   */
  def readMetaData(origPath: Path, configuration: Option[Configuration]): ParquetMetadata = {
    if (origPath == null) {
      throw new IllegalArgumentException("Unable to read Parquet metadata: path is null")
    }
    val job = new Job()
    val conf = configuration.getOrElse(ContextUtil.getConfiguration(job))
    val fs: FileSystem = origPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException(s"Incorrectly formatted Parquet metadata path $origPath")
    }
    val path = origPath.makeQualified(fs)
    if (!fs.getFileStatus(path).isDir) {
      throw new IllegalArgumentException(
        s"Expected $path for be a directory with Parquet files/metadata")
    }
    ParquetRelation.enableLogForwarding()
    val metadataPath = new Path(path, ParquetFileWriter.PARQUET_METADATA_FILE)
    // if this is a new table that was just created we will find only the metadata file
    if (fs.exists(metadataPath) && fs.isFile(metadataPath)) {
      ParquetFileReader.readFooter(conf, metadataPath)
    } else {
      // there may be one or more Parquet files in the given directory
      val footers = ParquetFileReader.readFooters(conf, fs.getFileStatus(path))
      // TODO: for now we assume that all footers (if there is more than one) have identical
      // metadata; we may want to add a check here at some point
      if (footers.size() == 0) {
        throw new IllegalArgumentException(s"Could not find Parquet metadata at path $path")
      }
      footers(0).getParquetMetadata
    }
  }

  /**
   * Reads in Parquet Metadata from the given path and tries to extract the schema
   * (Catalyst attributes) from the application-specific key-value map. If this
   * is empty it falls back to converting from the Parquet file schema which
   * may lead to an upcast of types (e.g., {byte, short} to int).
   *
   * @param origPath The path at which we expect one (or more) Parquet files.
   * @param conf The Hadoop configuration to use.
   * @return A list of attributes that make up the schema.
   */
  def readSchemaFromFile(origPath: Path, conf: Option[Configuration]): Seq[Attribute] = {
    val keyValueMetadata: java.util.Map[String, String] =
      readMetaData(origPath, conf)
        .getFileMetaData
        .getKeyValueMetaData
    if (keyValueMetadata.get(RowReadSupport.SPARK_METADATA_KEY) != null) {
      convertFromString(keyValueMetadata.get(RowReadSupport.SPARK_METADATA_KEY))
    } else {
      val attributes = convertToAttributes(
        readMetaData(origPath, conf).getFileMetaData.getSchema)
      log.warn(s"Falling back to schema conversion from Parquet types; result: $attributes")
      attributes
    }
  }
}
