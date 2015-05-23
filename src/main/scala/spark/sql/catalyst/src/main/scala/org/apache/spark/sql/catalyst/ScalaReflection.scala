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

package org.apache.spark.sql.catalyst

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types._

/**
 * Provides experimental support for generating catalyst schemas for scala objects.
 */
object ScalaReflection {
  import scala.reflect.runtime.universe._

  case class Schema(dataType: DataType, nullable: Boolean)

  /** Returns a Sequence of attributes for the given case class type. */
  def attributesFor[T: TypeTag]: Seq[Attribute] = schemaFor[T] match {
    case Schema(s: StructType, _) =>
      s.fields.map(f => AttributeReference(f.name, f.dataType, f.nullable)())
  }

  /** Returns a catalyst DataType and its nullability for the given Scala Type using reflection. */
  def schemaFor[T: TypeTag]: Schema = schemaFor(typeOf[T])

  /** Returns a catalyst DataType and its nullability for the given Scala Type using reflection. */
  def schemaFor(tpe: `Type`): Schema = tpe match {
    case t if t <:< typeOf[Option[_]] =>
      val TypeRef(_, _, Seq(optType)) = t
      Schema(schemaFor(optType).dataType, nullable = true)
    case t if t <:< typeOf[Product] =>
      val formalTypeArgs = t.typeSymbol.asClass.typeParams
      val TypeRef(_, _, actualTypeArgs) = t
      val params = t.member(nme.CONSTRUCTOR).asMethod.paramss
      Schema(StructType(
        params.head.map { p =>
          val Schema(dataType, nullable) =
            schemaFor(p.typeSignature.substituteTypes(formalTypeArgs, actualTypeArgs))
          StructField(p.name.toString, dataType, nullable)
        }), nullable = true)
    // Need to decide if we actually need a special type here.
    case t if t <:< typeOf[Array[Byte]] => Schema(BinaryType, nullable = true)
    case t if t <:< typeOf[Array[_]] =>
      sys.error(s"Only Array[Byte] supported now, use Seq instead of $t")
    case t if t <:< typeOf[Seq[_]] =>
      val TypeRef(_, _, Seq(elementType)) = t
      Schema(ArrayType(schemaFor(elementType).dataType), nullable = true)
    case t if t <:< typeOf[Map[_,_]] =>
      val TypeRef(_, _, Seq(keyType, valueType)) = t
      Schema(MapType(schemaFor(keyType).dataType, schemaFor(valueType).dataType), nullable = true)
    case t if t <:< typeOf[String] => Schema(StringType, nullable = true)
    case t if t <:< typeOf[Timestamp] => Schema(TimestampType, nullable = true)
    case t if t <:< typeOf[BigDecimal] => Schema(DecimalType, nullable = true)
    case t if t <:< typeOf[java.lang.Integer] => Schema(IntegerType, nullable = true)
    case t if t <:< typeOf[java.lang.Long] => Schema(LongType, nullable = true)
    case t if t <:< typeOf[java.lang.Double] => Schema(DoubleType, nullable = true)
    case t if t <:< typeOf[java.lang.Float] => Schema(FloatType, nullable = true)
    case t if t <:< typeOf[java.lang.Short] => Schema(ShortType, nullable = true)
    case t if t <:< typeOf[java.lang.Byte] => Schema(ByteType, nullable = true)
    case t if t <:< typeOf[java.lang.Boolean] => Schema(BooleanType, nullable = true)
    case t if t <:< definitions.IntTpe => Schema(IntegerType, nullable = false)
    case t if t <:< definitions.LongTpe => Schema(LongType, nullable = false)
    case t if t <:< definitions.DoubleTpe => Schema(DoubleType, nullable = false)
    case t if t <:< definitions.FloatTpe => Schema(FloatType, nullable = false)
    case t if t <:< definitions.ShortTpe => Schema(ShortType, nullable = false)
    case t if t <:< definitions.ByteTpe => Schema(ByteType, nullable = false)
    case t if t <:< definitions.BooleanTpe => Schema(BooleanType, nullable = false)
  }

  implicit class CaseClassRelation[A <: Product : TypeTag](data: Seq[A]) {

    /**
     * Implicitly added to Sequences of case class objects.  Returns a catalyst logical relation
     * for the the data in the sequence.
     */
    def asRelation: LocalRelation = {
      val output = attributesFor[A]
      LocalRelation(output, data)
    }
  }
}
