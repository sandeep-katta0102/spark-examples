package com.databricks.spark

import org.apache.hadoop.hive.ql.exec.UDF;
class SparkAddUDF extends UDF {

    def add_int(a: Int, b: Int): Int = {
        a + b
    }

}