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

import org.apache.hadoop.conf.Configuration
import org.apache.hudi.DataSourceReadOptions.{BEGIN_INSTANTTIME, END_INSTANTTIME, QUERY_TYPE, QUERY_TYPE_INCREMENTAL_OPT_VAL}
import org.apache.hudi.DataSourceWriteOptions.{DELETE_OPERATION_OPT_VAL, DELETE_PARTITION_OPERATION_OPT_VAL, HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, OPERATION, OPERATION_OPT_KEY, PARTITIONPATH_FIELD, PARTITIONPATH_FIELD_OPT_KEY, PARTITIONS_TO_DELETE, PAYLOAD_CLASS_OPT_KEY, PRECOMBINE_FIELD, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD, RECORDKEY_FIELD_OPT_KEY}
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieSparkUtils}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.common.config.HoodieStorageConfig
import org.apache.hudi.common.model.{EmptyHoodieRecordPayload, HoodieAvroPayload, OverwriteWithLatestAvroPayload, PartialUpdateAvroPayload, WriteOperationType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.config.HoodieWriteConfig.{TABLE_NAME, TBL_NAME}
import org.apache.hudi.examples.common.{HoodieExampleDataGenerator, HoodieExampleSparkUtils}
import org.apache.hudi.hive.{HiveSyncConfig, HiveSyncConfigHolder}
import org.apache.hudi.index.HoodieIndex.IndexType
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.sync.common.HoodieSyncConfig
import org.apache.hudi.utilities.UtilHelpers
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode.{Append, Overwrite}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}

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
  //  Logger.getLogger("org").setLevel(Level.ERROR)
  //  Logger.getLogger("io").setLevel(Level.ERROR)


  val spark = SparkSession.builder()
    .appName("testHoodie2Hive")
    .master("local[2]")
    .config("spark.sql.warehouse.dir", s"hdfs://hadoop-hdfs-namenode:9000/apps/hive/warehouse")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.parquet.writeLegacyFormat", true)
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.avro.datetimeRebaseModeInWrite", "CORRECTED")
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    .config("spark.sql.avro.datetimeRebaseModeInRead", "CORRECTED")
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]): Unit = {

//    if (args.length < 2) {
//      System.err.println("Usage: HoodieDataSourceExample <tablePath> <tableName>")
//      System.exit(1)
//    }
//    val tablePath = args(0)
//    val tableName = args(1)
//
//    val spark = HoodieExampleSparkUtils.defaultSparkSession("Hudi Spark basic example")
//
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

    val conf: Configuration = new Configuration()
    conf.addResource("core-site.xml")
    conf.addResource("hdfs-site.xml")

    val tablePath: String = "hdfs://hadoop-hdfs-namenode:9000/hudi/dww"
//    val tablePath: String = "hdfs://hadoop-hdfs-namenode:9000/hudi/tenant1/contact"
    val tableName = "dww"
    val databaseName = "default"

//    val metaClient: HoodieTableMetaClient = HoodieTableMetaClient.builder.setConf(conf).setBasePath(tablePath).build()
//    val schema = UtilHelpers.getSchemaFromLatestInstant(metaClient)
//    println(schema)

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    System.setProperty("user.name", "root")

//    testUpgradeHudi(databaseName,tableName,tablePath)
    testOperationByHudi(tablePath,databaseName,tableName,WriteOperationType.UPSERT.value)

//    val del_df = spark.
//      read
//      .format("org.apache.hudi")
//      .load(tablePath + "/default/*").withColumn("tenant_id",lit("default"))
//    del_df.show()

      val df = spark.
        read
        .format("org.apache.hudi")
        .option("hoodie.schema.on.read.enable","true")
        .load(tablePath + "/*")

      df.createOrReplaceTempView("dww")
      spark.sql("select * from dww").show()

    //    val commits = spark.sql("select distinct(_hoodie_commit_time) as commitTime from  test order by commitTime").map(k => k.getString(0))(org.apache.spark.sql.Encoders.STRING).take(50)
    ////    val beginTime: String = commits(0) // commit time we are interested in
    //    val beginTime: String = "20220714163914" // commit time we are interested in
    //    println(s"beginTime = ${beginTime}")
    //    val tripsIncrementalDF = spark.read.format("hudi").
    //      option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL).
    //      option(BEGIN_INSTANTTIME_OPT_KEY, beginTime.toLong - 1).
    //      option(END_INSTANTTIME_OPT_KEY, beginTime).
    //      load(tablePath)
    //    tripsIncrementalDF.createOrReplaceTempView("hudi_trips_incremental")
    //    spark.sql("select * from hudi_trips_incremental order by _hoodie_commit_time").show()


    //  testInsert(spark,basePath,tableName)
    //  testSchemaChanged(spark,basePath,tableName)

    spark.stop()
  }

  def sparkConf(): SparkConf = {
    val sparkConf = new SparkConf()
    if (HoodieSparkUtils.gteqSpark3_2) {
      sparkConf.set("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    }
    sparkConf
  }

  case class Test1(id: Int, ts: Int, name: String, tenant_id:String)
  case class Test2(id: Int, ts: Int, name: String, action: String,tenant_id:String)

  //0.11.1 to 0.1.4.0
  def testUpgradeHudi(databaseName:String,tableName:String,tablePath:String)={
    val df = spark.
      read
      .format("org.apache.hudi")
      .load(tablePath + "/default/*")
//
//    val df = spark.
//      read
//      .format("org.apache.hudi")
//      .load(tablePath + "/__HIVE_DEFAULT_PARTITION__/*")

    val df1 = df.drop("_hoodie_commit_time")
      .drop("_hoodie_commit_seqno")
      .drop("_hoodie_record_key")
      .drop("_hoodie_partition_path")
      .drop("_hoodie_file_name")
      .withColumn("tenant_id",lit(null))

    val list = df1.collectAsList()

    val schema = StructType(Array(
      StructField("id", IntegerType,true),
      StructField("ts", IntegerType,true),
      StructField("name", StringType,true),
      StructField("action", StringType,true),
      StructField("tenant_id", StringType,true)
    ))

    val df2 = spark.createDataFrame(list,schema)

    df2.show()

    df2.write.format("org.apache.hudi").
      options(getQuickstartWriteConfigs).
      option(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "ts").
      option(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "id").
      option(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "tenant_id").
      option("hoodie.skip.default.partition.validation",true).//升级时跳过default分区检查
//      option("hoodie.datasource.write.reconcile.schema", "true").
      option(HoodieWriteConfig.TBL_NAME.key, tableName).
      option(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), classOf[OverwriteNonPartialWithLatestAvroPayload].getName).
      option(DataSourceWriteOptions.OPERATION.key(), WriteOperationType.INSERT_OVERWRITE_TABLE.value()).
      option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key,"10").
      option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key,"10").
      mode(Overwrite).
      save(tablePath)
  }

  def testDelDefautlPartition(tablePath:String,databaseName:String,tableName:String) = {
    val del_df = spark.
      read
      .format("org.apache.hudi")
      .load(tablePath + "/default/*").withColumn("tenant_id",lit("default"))
    del_df.show()

    del_df.write
      .options(getQuickstartWriteConfigs)
//      .option("hoodie.datasource.write.operation", "delete")
      .option(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key, "ts")
      .option(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "id")
      .option(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "tenant_id")
      .option("hoodie.datasource.write.operation", WriteOperationType.DELETE_PARTITION.value)
      .option("hoodie.datasource.write.partitions.to.delete", "default")
      .option(HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key(), "true").
      option(HiveSyncConfigHolder.HIVE_URL.key(), "jdbc:hive2://hive:10000").
      option(HoodieSyncConfig.META_SYNC_TABLE_NAME.key(), tableName).
      option(HoodieSyncConfig.META_SYNC_DATABASE_NAME.key(), databaseName).
      option(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key(),"tenant_id").
      //      option("hoodie.datasource.write.hive_style_partitioning","true").
      option(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY,"org.apache.hudi.examples.spark.NonDefaultPartitionValueExtractor")
//            option("hoodie.clean.async", "true").
//            option("hoodie.cleaner.commits.retained", "1").
      .mode(Append).save(tablePath)
  }

  //
  def testOperationByHudi(tablePath: String, databaseName:String, tableName: String, operation: String) = {

//    val schema = StructType( Array(
//      StructField("id", IntegerType,true),
//      StructField("ts", IntegerType,true),
//      StructField("name", StringType,true),
//      StructField("action", StringType,true),
//    ))
//
//    val list = new ListBuffer[Row]();
//    for (i <- 1 to 2) {
//      list.+=:(Row(i, i, "hudi_0.14.0_" + i + "_3","hudi_0.14.0_" + i))
//    }
//    val df = spark.createDataFrame(list,schema)

    val list = new ListBuffer[Test1]()
    for (i <- 1 to 1){
//      list.+=:(Test2(i, i, "cc" + i,null,"0"))
      list.+=:(Test1(i, 0, "aa" + i,"0"))
    }
    val df = spark.createDataFrame(list)
    df.write.format("org.apache.hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD.key(), "ts").
      option(RECORDKEY_FIELD.key(), "id").
      option(PARTITIONPATH_FIELD.key(), "tenant_id").
      option(TBL_NAME.key(), tableName).
      option(HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.key(), String.valueOf(150)).
      option(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION.key(), "0.1").
      option(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key(), "125829120").
      option(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), classOf[PartialUpdateAvroPayload].getName). //这个payload无法实现更新成null的效果，而且由于在combineAndGetUpdateValue()中还判断了orderingFiled，导致df中的数据不一定能覆盖掉hudi中的数据(例如df中的数据的orderingVal比hudi中的小，就会取hudi中的数据，这和之前df中的数据一定会覆盖hudi数据不同)
      option("hoodie.datasource.write.schema.allow.auto.evolution.column.drop", "false").
      option("hoodie.datasource.write.reconcile.schema", "true").
//      option("hoodie.avro.schema.validate", "false").
      option("hoodie.datasource.write.drop.partition.columns", "false").
      option(OPERATION.key(), operation).
      //索引
      option(HoodieIndexConfig.INDEX_TYPE.key(), IndexType.BLOOM.name()).
      //hive
      option(HiveSyncConfig.HIVE_SYNC_ENABLED.key(), "true").
      option(HiveSyncConfig.HIVE_URL.key(), "jdbc:hive2://hive:10000").
      option(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key(), "false").
      option(HoodieSyncConfig.META_SYNC_TABLE_NAME.key(), tableName).
      option(HoodieSyncConfig.META_SYNC_DATABASE_NAME.key(), databaseName).
      option(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key(), "tenant_id").
      option(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY,"org.apache.hudi.examples.spark.NonDefaultPartitionValueExtractor").

      //      option("hoodie.embed.timeline.server", "false").
      //      option("hoodie.memory.merge.max.size",2048*1024*1024L).
      //      option("hoodie.clean.async", "true").
      //      option("hoodie.cleaner.commits.retained", "1").
      mode(Append).
      save(tablePath)
  }

  def testInsert(spark: SparkSession, basePath: String, tableName: String):Unit = {
    val schema = StructType( Array(
      StructField("rowId", StringType,true),
      StructField("partitionId", StringType,true),
      StructField("preComb", LongType,true),
      StructField("name", StringType,true),
      StructField("versionId", StringType,true),
      StructField("intToLong", IntegerType,true),//ok
      StructField("intToDouble", IntegerType,true),
      StructField("longToFloat", LongType,true),//ok
      // StructField("longToDouble", IntegerType,true),
      StructField("floatToDouble", FloatType,true)
    )) // 9 cols

    val data1 = Seq(Row("row_1", "part_0", 0L, "bob", "v_0", 0, 1, 1L, 1.1f),
      Row("row_2", "part_0", 0L, "john", "v_0", 0, 1, 2L, 1.2f),
      Row("row_3", "part_3", 0L, "tom", "v_0", 0, 1, 3L, 1.3f))

    var dfFromData1 = spark.createDataFrame(data1, schema)
    dfFromData1.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "preComb").
      option(RECORDKEY_FIELD_OPT_KEY, "rowId").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionId").
      option("hoodie.index.type","SIMPLE").
      option("hoodie.datasource.write.hive_style_partitioning", true).
      option(org.apache.hudi.config.HoodieWriteConfig.TBL_NAME.key(), tableName).
      mode(Overwrite).
      save(basePath)
  }

  def testSchemaChanged(spark: SparkSession, basePath: String, tableName: String):Unit = {
    // Int to double
    val newSchema = StructType( Array(
      StructField("rowId", StringType,true),
      StructField("partitionId", StringType,true),
      StructField("preComb", LongType,true),
      StructField("name", StringType,true),
      StructField("versionId", StringType,true),
      StructField("intToLong", IntegerType,true),
      StructField("intToDouble", DoubleType,true),
      StructField("longToFloat", LongType,true),
      // StructField("longToDouble", IntegerType,true),
      StructField("floatToDouble", FloatType,true)
    )) // 9 col

    val data2 = Seq(Row("row_2", "part_0", 5L, "john", "v_3", 3, 1D, 2l, 1.8f),
      Row("row_5", "part_0", 5L, "maroon", "v_2", 2, 1D, 2l, 1.8f),
      Row("row_9", "part_9", 5L, "michael", "v_2", 2, 1D, 2l, 1.8f))

    var dfFromData2 = spark.createDataFrame(data2, newSchema)

    dfFromData2.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "preComb").
      option(RECORDKEY_FIELD_OPT_KEY, "rowId").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionId").
      option("hoodie.datasource.write.hive_style_partitioning", true).
      option("hoodie.index.type","SIMPLE").
      option(org.apache.hudi.config.HoodieWriteConfig.TBL_NAME.key(), tableName).
      mode(Append).
      save(basePath)
  }

  /**
    * Generate some new trips, load them into a DataFrame and write the DataFrame into the Hudi dataset as below.
    */
  def insertData(spark: SparkSession, tablePath: String, tableName: String, dataGen: HoodieExampleDataGenerator[HoodieAvroPayload]): Unit = {

    val commitTime: String = System.currentTimeMillis().toString
    val inserts = dataGen.convertToStringList(dataGen.generateInserts(commitTime, 20))
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 1))
    df.write.format("hudi").
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
      format("hudi").
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
    df.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD.key, "ts").
      option(RECORDKEY_FIELD.key, "uuid").
      option(PARTITIONPATH_FIELD.key, "partitionpath").
      option(TBL_NAME.key, tableName).
      mode(Append).
      save(tablePath)
  }

  /**
   * Delete data based in data information.
   */
  def delete(spark: SparkSession, tablePath: String, tableName: String): Unit = {

    val roViewDF = spark.read.format("hudi").load(tablePath + "/*/*/*/*")
    roViewDF.createOrReplaceTempView("hudi_ro_table")
    val df = spark.sql("select uuid, partitionpath, ts from  hudi_ro_table limit 2")

    df.write.format("hudi").
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
   *  Delete the data of a single or multiple partitions.
   */
  def deleteByPartition(spark: SparkSession, tablePath: String, tableName: String): Unit = {
    val df = spark.emptyDataFrame
    df.write.format("hudi").
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
  def incrementalQuery(spark: SparkSession, tablePath: String, tableName: String): Unit = {
    import spark.implicits._
    val commits = spark.sql("select distinct(_hoodie_commit_time) as commitTime from hudi_ro_table order by commitTime").map(k => k.getString(0)).take(50)
    val beginTime = commits(commits.length - 2) // commit time we are interested in

    // incrementally query data
    val incViewDF = spark.
      read.
      format("hudi").
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
  def pointInTimeQuery(spark: SparkSession, tablePath: String, tableName: String): Unit = {
    import spark.implicits._
    val commits = spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_ro_table order by commitTime").map(k => k.getString(0)).take(50)
    val beginTime = "000" // Represents all commits > this time.
    val endTime = commits(commits.length - 2) // commit time we are interested in

    //incrementally query data
    val incViewDF = spark.read.format("hudi").
      option(QUERY_TYPE.key, QUERY_TYPE_INCREMENTAL_OPT_VAL).
      option(BEGIN_INSTANTTIME.key, beginTime).
      option(END_INSTANTTIME.key, endTime).
      load(tablePath)
    incViewDF.createOrReplaceTempView("hudi_incr_table")
    spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_incr_table where fare > 20.0").show()
  }
}
