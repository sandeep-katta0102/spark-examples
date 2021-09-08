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
package org.apache.spark.issues

import org.apache.spark.sql.SparkSession

object TestSerialization {

  /**
   * If we run using spark-submit it will pass. But if you using spark-shell then it will fail.
   * Same as SPARK-35272
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Test Serialization issue")
      .master("local")
      .getOrCreate()

    spark.streams.active

    case class Student(name: String)

    class NonSerializable() {

      def getText() : String = {
        """
          |[
          |{
          | "name": "test1"
          |},
          |{
          | "name": "test2"
          |}
          |]
          |""".stripMargin
      }
    }

    import spark.implicits._
    spark.read.json(Seq("jsonStr").toDF.map( x => x.json))

    import com.github.plokhotnyuk.jsoniter_scala.macros._
    import com.github.plokhotnyuk.jsoniter_scala.core._

    @transient val obj = new NonSerializable()
    val descriptors_string = obj.getText()

    val parsed_descriptors: Array[Student] =
      readFromString[Array[Student]](descriptors_string)(JsonCodecMaker.make)

    val broadcast_descriptors = spark.sparkContext.broadcast(parsed_descriptors)

    def foo(data: String): Seq[Any] = {

      import scala.collection.mutable.ArrayBuffer
      val res = new ArrayBuffer[String]()
      for (desc <- broadcast_descriptors.value) {
        res += desc.name
      }
      res
    }

    val data = spark.sparkContext.parallelize(Array("1", "2", "3")).map(x => foo(x)).collect()
//    // scalastyle: off println
//    data.foreach(println(_))
//    // scalastyle: on println
  }

}
