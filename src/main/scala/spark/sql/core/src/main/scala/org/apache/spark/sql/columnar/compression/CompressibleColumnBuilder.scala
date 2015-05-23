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

package org.apache.spark.sql.columnar.compression

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.sql.{Logging, Row}
import org.apache.spark.sql.catalyst.types.NativeType
import org.apache.spark.sql.columnar.{ColumnBuilder, NativeColumnBuilder}

/**
 * A stackable trait that builds optionally compressed byte buffer for a column.  Memory layout of
 * the final byte buffer is:
 * {{{
 *    .--------------------------- Column type ID (4 bytes)
 *    |   .----------------------- Null count N (4 bytes)
 *    |   |   .------------------- Null positions (4 x N bytes, empty if null count is zero)
 *    |   |   |     .------------- Compression scheme ID (4 bytes)
 *    |   |   |     |   .--------- Compressed non-null elements
 *    V   V   V     V   V
 *   +---+---+-----+---+---------+
 *   |   |   | ... |   | ... ... |
 *   +---+---+-----+---+---------+
 *    \-----------/ \-----------/
 *       header         body
 * }}}
 */
private[sql] trait CompressibleColumnBuilder[T <: NativeType]
  extends ColumnBuilder with Logging {

  this: NativeColumnBuilder[T] with WithCompressionSchemes =>

  import CompressionScheme._

  var compressionEncoders: Seq[Encoder[T]] = _

  abstract override def initialize(initialSize: Int, columnName: String, useCompression: Boolean) {
    compressionEncoders =
      if (useCompression) {
        schemes.filter(_.supports(columnType)).map(_.encoder[T])
      } else {
        Seq(PassThrough.encoder)
      }
    super.initialize(initialSize, columnName, useCompression)
  }

  protected def isWorthCompressing(encoder: Encoder[T]) = {
    encoder.compressionRatio < 0.8
  }

  private def gatherCompressibilityStats(row: Row, ordinal: Int) {
    val field = columnType.getField(row, ordinal)

    var i = 0
    while (i < compressionEncoders.length) {
      compressionEncoders(i).gatherCompressibilityStats(field, columnType)
      i += 1
    }
  }

  abstract override def appendFrom(row: Row, ordinal: Int) {
    super.appendFrom(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      gatherCompressibilityStats(row, ordinal)
    }
  }

  abstract override def build() = {
    val rawBuffer = super.build()
    val encoder: Encoder[T] = {
      val candidate = compressionEncoders.minBy(_.compressionRatio)
      if (isWorthCompressing(candidate)) candidate else PassThrough.encoder
    }

    val headerSize = columnHeaderSize(rawBuffer)
    val compressedSize = if (encoder.compressedSize == 0) {
      rawBuffer.limit - headerSize
    } else {
      encoder.compressedSize
    }

    // Reserves 4 bytes for compression scheme ID
    val compressedBuffer = ByteBuffer
      .allocate(headerSize + 4 + compressedSize)
      .order(ByteOrder.nativeOrder)

    copyColumnHeader(rawBuffer, compressedBuffer)

    logger.info(s"Compressor for [$columnName]: $encoder, ratio: ${encoder.compressionRatio}")
    encoder.compress(rawBuffer, compressedBuffer, columnType)
  }
}
