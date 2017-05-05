package com.flipkart

import java.sql.Timestamp

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by karan.verma on 28/04/17.
  */
object ThreeMonthPvs {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("Accounts Data")
    val sc = new SparkContext(conf)

    System.setProperty("hive.metastore.uris", "thrift://prod-fdphadoop-hive-ha-0002:9083,thrift://prod-fdphadoop-hive-ha-0003:9083,thrift://prod-fdphadoop-hive-ha-0004:9083,thrift://prod-fdphadoop-hive-ha-0001:9083")
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    sqlContext.setConf("hive.metastore.uris", "thrift://prod-fdphadoop-hive-ha-0002:9083,thrift://prod-fdphadoop-hive-ha-0003:9083,thrift://prod-fdphadoop-hive-ha-0004:9083,thrift://prod-fdphadoop-hive-ha-0001:9083")
    sqlContext.setConf("dfs.client.failover.proxy.provider.bheema", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    sqlContext.setConf("dfs.ha.namenodes.bheema", "nn1,nn2")
    sqlContext.setConf("dfs.namenode.http-address.bheema.nn1", "prod-fdphadoop-bheema-nn-0001:50070")
    sqlContext.setConf("dfs.namenode.http-address.bheema.nn2", "prod-fdphadoop-bheema-nn-0002:50070")
    sqlContext.setConf("dfs.namenode.rpc-address.bheema.nn1", "prod-fdphadoop-bheema-nn-0001:8020")
    sqlContext.setConf("dfs.namenode.rpc-address.bheema.nn2", "prod-fdphadoop-bheema-nn-0002:8020")
    sqlContext.setConf("dfs.nameservices", "bheema,athena-prod")
    sqlContext.setConf("dfs.ha.automatic-failover.enabled", "true")

    val sql = sqlContext.sql("select * from three_month_pvs limit 2")

    println("Total Users : " +sql.count())

    val mapRdd = sql.rdd.map(event => {
      (event(0).asInstanceOf[String], event(1).asInstanceOf[String], event(2).asInstanceOf[Timestamp])
    })

    mapRdd.saveAsTextFile("/tmp/accountData/")
  }

}
