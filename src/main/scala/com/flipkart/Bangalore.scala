package com.flipkart

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by karan.verma on 01/05/17.
  */
object Bangalore {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("Accounts Data")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    write_filtered_as_csv(sqlContext)
  }

  def read_and_filter_bangalore(sqlContext: SQLContext) : DataFrame = {
    val df = sqlContext.read.json("/tmp/raw_with_ssid_1/*")
    df.show()
    val schema = df.schema
    val filtered_rdd = df.rdd.filter(row => EnvelopeCheck.contained_in_bangalore(row(3).asInstanceOf[Double], row(2).asInstanceOf[Double]))
    val filtered_df = sqlContext.createDataFrame(filtered_rdd, schema)
    filtered_df.show()
    println(filtered_df.count())
    filtered_df.write.format("json").save("/tmp/filtered_bangalore")
    filtered_df
  }

  def write_filtered_as_csv(sqlContext: SQLContext) : Unit = {
    val df = sqlContext.read.json("/tmp/filtered_bangalore")
    df.write.format("com.databricks.spark.csv").option("header", "true")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save("/tmp/bangalore_filtered.csv.gz")
  }

 def raw_data_frame(sqlContext: HiveContext) : DataFrame = {
    sqlContext.sql("ADD JAR /usr/share/athena-jars/json-serde-1.3-SNAPSHOT-jar-with-dependencies.jar")
    val raw = sqlContext.sql("select data.accountid as accountid, data.lastmodified as eventtime, data.networktype as networktype, data.networkidentifier as networkidentifier, parent.data.visit.location.latitude as latitude, parent.data.visit.location.longitude as longitude from bigfoot_journal.dart_fkint_cp_ca_common_connectinfo_2_0 where data.accountid is not null and parent.data.visit.location.latitude is not null and parent.data.visit.location.longitude is not null and day > 20160101 ")
    raw.write.format("json").save("/tmp/raw_with_ssid")
    return raw
 }

// def convert_eventime(sqlContext: SQLContext) : DataFrame = {
//
// }
}

