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

package org.apache.spark.datasource

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object TestParquetFilterInJoin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Test Count")
      .master("local")
      .getOrCreate()

    import spark.implicits._


    val factData = Seq[(Int, Int, Int, Int)](
      (1000, 1, 1, 10),
      (1010, 2, 1, 10),
      (1020, 2, 1, 10),
      (1030, 3, 2, 10),
      (1040, 3, 2, 50),
      (1050, 3, 2, 50),
      (1060, 3, 2, 50),
      (1070, 4, 2, 10),
      (1080, 4, 3, 20),
      (1090, 4, 3, 10),
      (1100, 4, 3, 10),
      (1110, 5, 3, 10),
      (1120, 6, 4, 10),
      (1130, 7, 4, 50),
      (1140, 8, 4, 50),
      (1150, 9, 1, 20),
      (1160, 10, 1, 20),
      (1170, 11, 1, 30),
      (1180, 12, 2, 20),
      (1190, 13, 2, 20),
      (1200, 14, 3, 40),
      (1200, 15, 3, 70),
      (1210, 16, 4, 10),
      (1220, 17, 4, 20),
      (1230, 18, 4, 20),
      (1240, 19, 5, 40),
      (1250, 20, 5, 40),
      (1260, 21, 5, 40),
      (1270, 22, 5, 50),
      (1280, 23, 1, 50),
      (1290, 24, 1, 50),
      (1300, 25, 1, 50)
    )

    val storeData = Seq[(Int, String, String)](
      (1, "North-Holland", "NL"),
      (2, "South-Holland", "NL"),
      (3, "Bavaria", "DE"),
      (4, "California", "US"),
      (5, "Texas", "US"),
      (6, "Texas", "US"))

    factData.toDF("date_id", "store_id", "product_id", "units_sold")
      .write.partitionBy("store_id").mode("overwrite")
      .parquet("/Users/sandeep.katta/Downloads/parquet/partitioned/factData")

    storeData.toDF("store_id", "state_province", "country")
      .write.partitionBy("store_id").mode("overwrite")
      .parquet("/Users/sandeep.katta/Downloads/parquet/partitioned/storeData")

    val factSchema = new StructType().add("date_id", IntegerType)
      .add("store_id", IntegerType)
      .add("product_id", IntegerType)
      .add("units_sold", IntegerType)

    val storeSchema = new StructType().add("store_id", IntegerType)
      .add("state_province", StringType)
      .add("country", StringType)

    val df1 = spark.read.schema(factSchema)
      .parquet("/Users/sandeep.katta/Downloads/parquet/partitioned/factData")

    val df2 = spark.read.schema(storeSchema)
      .parquet("/Users/sandeep.katta/Downloads/parquet/partitioned/storeData")

    // partition filter should be pushed down to filescan
    val result = df1.join(df2, Seq("store_id"), "left")


    println(result.count())
  }
}
