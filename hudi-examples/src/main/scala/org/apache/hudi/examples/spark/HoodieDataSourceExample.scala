/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.examples.spark

import org.apache.hudi.DataSourceReadOptions.{BEGIN_INSTANTTIME, END_INSTANTTIME, QUERY_TYPE, QUERY_TYPE_INCREMENTAL_OPT_VAL}
import org.apache.hudi.DataSourceWriteOptions.{DELETE_OPERATION_OPT_VAL, DELETE_PARTITION_OPERATION_OPT_VAL, OPERATION, OPERATION_OPT_KEY, PARTITIONPATH_FIELD, PARTITIONPATH_FIELD_OPT_KEY, PARTITIONS_TO_DELETE, PAYLOAD_CLASS_NAME, PAYLOAD_CLASS_OPT_KEY, PRECOMBINE_FIELD, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD, RECORDKEY_FIELD_OPT_KEY}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.common.model.{HoodieAvroPayload, OverwriteNonDefaultsWithLatestAvroPayload, WriteOperationType}
import org.apache.hudi.config.HoodieCompactionConfig
import org.apache.hudi.config.HoodieCompactionConfig.COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE
import org.apache.hudi.config.HoodieWriteConfig.{KEYGENERATOR_CLASS_NAME, TABLE_NAME, TBL_NAME}
import org.apache.hudi.examples.common.{HoodieExampleDataGenerator, HoodieExampleSparkUtils}
import org.apache.hudi.examples.spark.HoodieDataSourceExample.{TestPartition1, testInsertOverwrite2hudi}
import org.apache.hudi.keygen.SimpleKeyGenerator
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode.{Append, Overwrite}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hudi.command.SqlKeyGenerator

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
 * Simple examples of [[org.apache.hudi.DefaultSource]]
 *
 * To run this example, you should
 *   1. For running in IDE, set VM options `-Dspark.master=local[2]`
 *      2. For running in shell, using `spark-submit`
 *
 * Usage: HoodieWriteClientExample <tablePath> <tableName>.
 * <tablePath> and <tableName> describe root path of hudi and table name
 * for example, `HoodieDataSourceExample file:///tmp/hoodie/hudi_cow_table hudi_cow_table`
 */
object HoodieDataSourceExample {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("io").setLevel(Level.ERROR)

  case class Test(id: Int, ts: Int, name: String, action: String)

  case class TestPartition(id: Int, ts: Int, name: String, action: String, date: String)

  case class TestPartition1(id: Int, ts: Int, name: String, date: String)

  case class TestPartition2(id: Int, ts: Int, name1: String, date: String)

  case class DeletePartition2(id: Int, ts: Int, _hoodie_is_deleted: Boolean)

  case class DeletePartition(id: Int)

  case class DeletePartition1(id: Int, ts: Int)

  case class Contact(id: Long, ts: Long, gender: String)

  def main(args: Array[String]): Unit = {

    //    if (args.length < 2) {
    //      System.err.println("Usage: HoodieDataSourceExample <tablePath> <tableName>")
    //      System.exit(1)
    //    }
    //    val tablePath = args(0)
    //    val tableName = args(1)
//            val tablePath = "/Users/edz/hudi/9/test1"
//    val tablePath = "hdfs://localhost:9000/hudi/8/hudi8_partition_date"
        val tablePath = "hdfs://localhost:9000/user/hive/warehouse/hudi_table1"
    val tableName = "hudi_table1"

    val spark = HoodieExampleSparkUtils.defaultSparkSession("Hudi Spark basic example")

    testInsertOverwrite2hudi(spark, tablePath, tableName)

    val roViewDF1 = spark.
      read
      .option("spark.sql.parquet.mergeSchema", false)
      .format("org.apache.hudi").
      load(tablePath + "/*")
    println("after size = " + roViewDF1.count())
    roViewDF1.createOrReplaceTempView("test1")
    spark.sql("select * from test1").show()
    //    val dataGen = new HoodieExampleDataGenerator[HoodieAvroPayload]
    //    insertData(spark, tablePath, tableName, dataGen)
    //    updateData(spark, tablePath, tableName, dataGen)
    //    queryData(spark, tablePath, tableName, dataGen)
    //
    //    incrementalQuery(spark, tablePath, tableName)
    //    pointInTimeQuery(spark, tablePath, tableName)
    //
    //    delete(spark, tablePath, tableName)
    //    deleteByPartition(spark, tablePath, tableName)

    spark.stop()
  }

  def testInsertOverwrite2hudi(spark: SparkSession, tablePath: String, tableName: String) = {
    val start = System.currentTimeMillis()
    val list = new ListBuffer[TestPartition1]()
    list.+=:(TestPartition1(1, 2, "ahah", "2020-10-10"))
//    list.+=:(TestPartition1(2, 1, "a2", "2015-01-02"))
    val df = spark.createDataFrame(list)
    df.write.format("org.apache.hudi").
      options(getQuickstartWriteConfigs).
      option(KEYGENERATOR_CLASS_NAME.key(),classOf[SqlKeyGenerator].getCanonicalName).
      option(KEYGENERATOR_CLASS_NAME.key(),classOf[SimpleKeyGenerator].getCanonicalName).
      option(PRECOMBINE_FIELD.key(), "ts").
      option(RECORDKEY_FIELD.key(), "id").
      option(PARTITIONPATH_FIELD.key(), "date").
      option(TBL_NAME.key(), tableName).
      option(PAYLOAD_CLASS_NAME.key(), classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName).
      option(OPERATION.key(), WriteOperationType.UPSERT.value()).
      option(HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.key(), String.valueOf(300)).
      option(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key(), "true").
    //如果加下面这个参数，那么存储的目录为 date=2015%2F01%2F01 ，date=2015%2F01%2F02
    //如果不加下面这个参数，那么存储的目录为 date=2015/01/01 ，date=2015/01/02
//      option(KeyGeneratorOptions.URL_ENCODE_PARTITIONING.key(), "true").
      mode(Append).
      save(tablePath)
    val end = System.currentTimeMillis()
    println("cost time = " + (end - start) / 1000)
  }

  /**
   * Generate some new trips, load them into a DataFrame and write the DataFrame into the Hudi dataset as below.
   */
  def insertData(spark: SparkSession, tablePath: String, tableName: String, dataGen: HoodieExampleDataGenerator[HoodieAvroPayload]): Unit = {

    val commitTime: String = System.currentTimeMillis().toString
    val inserts = dataGen.convertToStringList(dataGen.generateInserts(commitTime, 20))
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 1))
    df.write.format("org.apache.hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD.key, "ts").
      option(RECORDKEY_FIELD.key, "uuid").
      option(PARTITIONPATH_FIELD.key, "partitionpath").
      option(TBL_NAME.key, tableName).
      mode(Overwrite).
      save(tablePath)
  }

  /**
   * Load the data files into a DataFrame.
   */
  def queryData(spark: SparkSession, tablePath: String, tableName: String, dataGen: HoodieExampleDataGenerator[HoodieAvroPayload]): Unit = {
    val roViewDF = spark.
      read.
      format("org.apache.hudi").
      load(tablePath + "/*/*/*/*")

    roViewDF.createOrReplaceTempView("hudi_ro_table")

    spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_ro_table where fare > 20.0").show()
    //  +-----------------+-------------------+-------------------+---+
    //  |             fare|          begin_lon|          begin_lat| ts|
    //  +-----------------+-------------------+-------------------+---+
    //  |98.88075495133515|0.39556048623031603|0.17851135255091155|0.0|
    //  ...

    spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_ro_table").show()
    //  +-------------------+--------------------+----------------------+-------------------+--------------------+------------------+
    //  |_hoodie_commit_time|  _hoodie_record_key|_hoodie_partition_path|              rider|              driver|              fare|
    //  +-------------------+--------------------+----------------------+-------------------+--------------------+------------------+
    //  |     20191231181501|31cafb9f-0196-4b1...|            2020/01/02|rider-1577787297889|driver-1577787297889| 98.88075495133515|
    //  ...
  }

  /**
   * This is similar to inserting new data. Generate updates to existing trips using the data generator,
   * load into a DataFrame and write DataFrame into the hudi dataset.
   */
  def updateData(spark: SparkSession, tablePath: String, tableName: String, dataGen: HoodieExampleDataGenerator[HoodieAvroPayload]): Unit = {

    val commitTime: String = System.currentTimeMillis().toString
    val updates = dataGen.convertToStringList(dataGen.generateUpdates(commitTime, 10))
    val df = spark.read.json(spark.sparkContext.parallelize(updates, 1))
    df.write.format("org.apache.hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD.key, "ts").
      option(RECORDKEY_FIELD.key, "uuid").
      option(PARTITIONPATH_FIELD.key, "partitionpath").
      option(TBL_NAME.key, tableName).
      mode(Append).
      save(tablePath)
  }

  /**
   * Deleta data based in data information.
   */
  def delete(spark: SparkSession, tablePath: String, tableName: String): Unit = {

    val roViewDF = spark.read.format("org.apache.hudi").load(tablePath + "/*/*/*/*")
    roViewDF.createOrReplaceTempView("hudi_ro_table")
    val df = spark.sql("select uuid, partitionpath, ts from  hudi_ro_table limit 2")

    df.write.format("org.apache.hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD.key, "ts").
      option(RECORDKEY_FIELD.key, "uuid").
      option(PARTITIONPATH_FIELD.key, "partitionpath").
      option(TBL_NAME.key, tableName).
      option(OPERATION.key, DELETE_OPERATION_OPT_VAL).
      mode(Append).
      save(tablePath)
  }

  /**
   * Delete the data of a single or multiple partitions.
   */
  def deleteByPartition(spark: SparkSession, tablePath: String, tableName: String): Unit = {
    val df = spark.emptyDataFrame
    df.write.format("org.apache.hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD.key, "ts").
      option(RECORDKEY_FIELD.key, "uuid").
      option(PARTITIONPATH_FIELD.key, "partitionpath").
      option(TBL_NAME.key, tableName).
      option(OPERATION.key, DELETE_PARTITION_OPERATION_OPT_VAL).
      option(PARTITIONS_TO_DELETE.key(), HoodieExampleDataGenerator.DEFAULT_PARTITION_PATHS.mkString(",")).
      mode(Append).
      save(tablePath)
  }

  /**
   * Hudi also provides capability to obtain a stream of records that changed since given commit timestamp.
   * This can be achieved using Hudi’s incremental view and providing a begin time from which changes need to be streamed.
   * We do not need to specify endTime, if we want all changes after the given commit (as is the common case).
   */
  def incrementalQuery(spark: SparkSession, tablePath: String, tableName: String) {
    import spark.implicits._
    val commits = spark.sql("select distinct(_hoodie_commit_time) as commitTime from hudi_ro_table order by commitTime").map(k => k.getString(0)).take(50)
    val beginTime = commits(commits.length - 2) // commit time we are interested in

    // incrementally query data
    val incViewDF = spark.
      read.
      format("org.apache.hudi").
      option(QUERY_TYPE.key, QUERY_TYPE_INCREMENTAL_OPT_VAL).
      option(BEGIN_INSTANTTIME.key, beginTime).
      load(tablePath)
    incViewDF.createOrReplaceTempView("hudi_incr_table")
    spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from hudi_incr_table where fare > 20.0").show()
  }

  /**
   * Lets look at how to query data as of a specific time.
   * The specific time can be represented by pointing endTime to a specific commit time
   * and beginTime to “000” (denoting earliest possible commit time).
   */
  def pointInTimeQuery(spark: SparkSession, tablePath: String, tableName: String) {
    import spark.implicits._
    val commits = spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_ro_table order by commitTime").map(k => k.getString(0)).take(50)
    val beginTime = "000" // Represents all commits > this time.
    val endTime = commits(commits.length - 2) // commit time we are interested in

    //incrementally query data
    val incViewDF = spark.read.format("org.apache.hudi").
      option(QUERY_TYPE.key, QUERY_TYPE_INCREMENTAL_OPT_VAL).
      option(BEGIN_INSTANTTIME.key, beginTime).
      option(END_INSTANTTIME.key, endTime).
      load(tablePath)
    incViewDF.createOrReplaceTempView("hudi_incr_table")
    spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_incr_table where fare > 20.0").show()
  }
}
