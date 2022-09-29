package com.spark.extensions

import org.apache.spark.sql.SparkSessionExtensions

class SparkExtensionTest extends  ((SparkSessionExtensions) => Unit) {

  override def apply(ext: SparkSessionExtensions): Unit = {
   ext.injectOptimizerRule(TablePermissions)
  }
}
