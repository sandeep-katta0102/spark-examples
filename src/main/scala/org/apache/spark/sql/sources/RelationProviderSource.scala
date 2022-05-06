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

package org.apache.spark.sql.sources

import org.apache.spark.rdd.RDD
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.util.Locale

class RelationProviderSource extends RelationProvider  {
  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {
    SimpleFilteredScan(parameters("from").toInt, parameters("to").toInt)(sqlContext.sparkSession)
  }
}

case class SimpleFilteredScan(from: Int, to: Int)(@transient val sparkSession: SparkSession)
  extends BaseRelation
    with PrunedFilteredScan
    with Logging {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType =
    StructType(
      StructField("a", IntegerType, nullable = false) ::
        StructField("b", IntegerType, nullable = false) ::
        StructField("c", StringType, nullable = false) :: Nil)

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    def unhandled(filter: Filter): Boolean = {
      filter match {
        case EqualTo(col, _) => col == "b"
        case EqualNullSafe(col, _) => col == "b"
        case LessThan(col, _: Int) => col == "b"
        case LessThanOrEqual(col, _: Int) => col == "b"
        case GreaterThan(col, _: Int) => col == "b"
        case GreaterThanOrEqual(col, _: Int) => col == "b"
        case In(col, _) => col == "b"
        case IsNull(col) => col == "b"
        case IsNotNull(col) => col == "b"
        case Not(pred) => unhandled(pred)
        case And(left, right) => unhandled(left) || unhandled(right)
        case Or(left, right) => unhandled(left) || unhandled(right)
        case _ => false
      }
    }

    filters.filter(unhandled)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    logError("************* Build Scan is called ************** ")
    val rowBuilders = requiredColumns.map {
      case "a" => (i: Int) => Seq(i)
      case "b" => (i: Int) => Seq(i * 2)
      case "c" => (i: Int) =>
        val c = (i - 1 + 'a').toChar.toString
        Seq(c * 5 + c.toUpperCase(Locale.ROOT) * 5)
    }


    // Predicate test on integer column
    def translateFilterOnA(filter: Filter): Int => Boolean = filter match {
      case EqualTo("a", v) => (a: Int) => a == v
      case EqualNullSafe("a", v) => (a: Int) => a == v
      case LessThan("a", v: Int) => (a: Int) => a < v
      case LessThanOrEqual("a", v: Int) => (a: Int) => a <= v
      case GreaterThan("a", v: Int) => (a: Int) => a > v
      case GreaterThanOrEqual("a", v: Int) => (a: Int) => a >= v
      case In("a", values) => (a: Int) => values.map(_.asInstanceOf[Int]).toSet.contains(a)
      case IsNull("a") => (a: Int) => false // Int can't be null
      case IsNotNull("a") => (a: Int) => true
      case Not(pred) => (a: Int) => !translateFilterOnA(pred)(a)
      case And(left, right) => (a: Int) =>
        translateFilterOnA(left)(a) && translateFilterOnA(right)(a)
      case Or(left, right) => (a: Int) =>
        translateFilterOnA(left)(a) || translateFilterOnA(right)(a)
      case _ => (a: Int) => true
    }

    // Predicate test on string column
    def translateFilterOnC(filter: Filter): String => Boolean = filter match {
      case StringStartsWith("c", v) => _.startsWith(v)
      case StringEndsWith("c", v) => _.endsWith(v)
      case StringContains("c", v) => _.contains(v)
      case EqualTo("c", v: String) => _.equals(v)
      case EqualTo("c", _: UTF8String) => sys.error("UTF8String should not appear in filters")
      case In("c", values) => (s: String) => values.map(_.asInstanceOf[String]).toSet.contains(s)
      case _ => (c: String) => true
    }

    def eval(a: Int) = {
      val c = (a - 1 + 'a').toChar.toString * 5 +
        (a - 1 + 'a').toChar.toString.toUpperCase(Locale.ROOT) * 5
      filters.forall(translateFilterOnA(_)(a)) && filters.forall(translateFilterOnC(_)(c))
    }

    sparkSession.sparkContext.parallelize(from to to).filter(eval).map(i =>
      Row.fromSeq(rowBuilders.map(_(i)).reduceOption(_ ++ _).getOrElse(Seq.empty)))
  }
}