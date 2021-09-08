package org.apache.spark.issues

import org.apache.spark.sql.SparkSession

object TestThis {

  def main(args: Array[String]): Unit = {

    val test = Seq(("2020","2020-1","2020-01-01")).flatMap( record => record match {
      case (year, month, day) => for {
        product <- Seq("aa", "bb").toList
      } yield (year, month, day, product)
    })

    val data = test.flatMap( record => record match {
      case (year, month, day, product) => for {
        index <- (1 to 100).toList
      } yield (year, month, day, product, index)
    })

    println(data.head)

  }

}
