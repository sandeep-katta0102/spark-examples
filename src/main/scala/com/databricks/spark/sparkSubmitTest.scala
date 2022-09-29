package com.databricks.spark

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object sparkSubmitTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local")
      .getOrCreate()

    val config = ConfigFactory.defaultApplication()
    val tableName = config.getString("tables.name")
    println(s"tableName is $tableName")
    val df = spark.sql(s"select * from $tableName")
    df.show(false)
  }
}
