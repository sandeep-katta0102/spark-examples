package com.databricks.spark

import org.apache.spark.sql.SparkSession

import org.openx.data.jsonserde.json.JSONObject
object TestJarJob {

  def main(args: Array[String]): Unit = {
    println("Application starting.....................")
    val spark = SparkSession
      .builder
      .getOrCreate()

    import spark.implicits._
    val df = spark.range(0, 10000).toDF("id")
    df.mapPartitions {
      partitions => {
        val jsonObj = new JSONObject()
        println(s"json object constructed is ${jsonObj}")
        partitions.map(_.mkString("$$$"))
      }
    }.collect()

    val sleepDuration = 5*60*1000L //5 minutes
    Thread.sleep(sleepDuration)
    println("Application exiting.....................")
  }
}
