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

package org.apache.spark.sql.streamingsource

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SparkSession}

/**
 * CustomSource is like provider e.g. Kinesis, kafka etc
 * It is required to extend Souce interface
 * First Spark will call getOffset API if it has new data then only getBatch API will called
 * @param sqlContext
 */
private class CustomSource(sqlContext: SQLContext) extends Source with Logging {

  @volatile var currentOffset = 0L
  @volatile var currentBatchCount = 0
  val totalBatchCount = 10

  private def sc = sqlContext.sparkContext


  /** Returns the schema of the data from this source */
  override def schema: StructType = {
    new StructType().add("id", LongType)
  }

  /**
   * Returns the maximum available offset for this source.
   * Returns `None` if this source has never received any data.
   */
  override def getOffset: Option[Offset] = {
    // for every batch pushing 100 records
    Some(LongOffset(currentOffset + 100))
  }

  /**
   * Returns the data that is between the offsets (`start`, `end`]. When `start` is `None`,
   * then the batch should begin with the first record. This method must always return the
   * same data for a particular `start` and `end` pair; even after the Source has been restarted
   * on a different node.
   *
   * Higher layers will always call this method with a value of `start` greater than or equal
   * to the last value passed to `commit` and a value of `end` less than or equal to the
   * last value returned by `getOffset`
   *
   * It is possible for the [[Offset]] type to be a [[SerializedOffset]] when it was
   * obtained from the log. Moreover, [[StreamExecution]] only compares the [[Offset]]
   * JSON representation to determine if the two objects are equal. This could have
   * ramifications when upgrading [[Offset]] JSON formats i.e., two equivalent [[Offset]]
   * objects could differ between version. Consequently, [[StreamExecution]] may call
   * this method with two such equivalent [[Offset]] objects. In which case, the [[Source]]
   * should return an empty [[DataFrame]]
   */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    currentBatchCount += 1
    logInfo(s"sending data from ${start} to ${end}")
    val spark = SparkSession.getActiveSession.get
    val startIndex = start.map(getOffsetValue).getOrElse(0L)
    val endIndex = getOffsetValue(end)
    val rows = (startIndex to endIndex).map(data => InternalRow(data))
    // I will not increase the offset if desired batch count is reached
    if (currentBatchCount < totalBatchCount) {
      currentOffset = getOffsetValue(end)
    }

    Dataset.ofRows(spark, LocalRelation(schema.toAttributes, rows,
      isStreaming = true))
  }

  /**
   * Stop this source and free any resources it has allocated.
   */
  override def stop(): Unit = {

  }

  private def getOffsetValue(offset: Offset): Long = {
    offset match {
      case s: SerializedOffset => LongOffset(s).offset
      case l: LongOffset => l.offset
      case _ => throw new IllegalArgumentException("incorrect offset type: " + offset)
    }
  }

}
