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
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.{IntegerType, StructType}


class CustomSourceProvider extends DataSourceRegister with StreamSourceProvider with Logging {


  /**
   * Returns the name and schema of the source that can be used to continually read data.
   * parameters are the options passed using .option method
   * @since 2.0.0
   */
  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType], providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {

    // Take the passed schema if not present than I am using simple schema with 1 column with of type Integer
    val updatedSchema: StructType = schema match {
      case Some(passedSchema) => passedSchema
      case None => new StructType().add("id", IntegerType)
    }
    ("schemaDef", updatedSchema)
  }

  /**
   * @since 2.0.0
   */
  override def createSource(sqlContext: SQLContext, metadataPath: String,
                            schema: Option[StructType],
                            providerName: String, parameters: Map[String, String]): Source = {
     new CustomSource(sqlContext)
  }

  /**
   * The string that represents the format that this data source provider uses. This is
   * overridden by children to provide a nice alias for the data source. For example:
   *
   * {{{
   *   override def shortName(): String = "parquet"
   * }}}
   *
   * @since 1.5.0
   */
  override def shortName(): String = "customsource"

}
