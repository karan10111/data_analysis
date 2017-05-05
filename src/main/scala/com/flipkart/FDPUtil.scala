package com.flipkart

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.format.DateTimeFormat

object FDPUtil {
  def getHiveContext(sc: SparkContext) : HiveContext = {
    System.setProperty("hive.metastore.uris","thrift://prod-bheema-hive-2-1-0-0001:9083,thrift://prod-bheema-hive-2-1-0-0002:9083,thrift://prod-bheema-hive-2-1-0-0003:9083")
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    sqlContext.setConf("hive.metastore.uris", "thrift://prod-bheema-hive-2-1-0-0001:9083,thrift://prod-bheema-hive-2-1-0-0002:9083,thrift://prod-bheema-hive-2-1-0-0003:9083")
    sqlContext.setConf("dfs.client.failover.proxy.provider.bheema","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    sqlContext.setConf("dfs.ha.namenodes.bheema", "nn1,nn2")
    sqlContext.setConf("dfs.namenode.http-address.bheema.nn1", "prod-fdphadoop-bheema-nn-0001:50070")
    sqlContext.setConf("dfs.namenode.http-address.bheema.nn2", "prod-fdphadoop-bheema-nn-0002:50070")
    sqlContext.setConf("dfs.namenode.rpc-address.bheema.nn1", "prod-fdphadoop-bheema-nn-0001:8020")
    sqlContext.setConf("dfs.namenode.rpc-address.bheema.nn2", "prod-fdphadoop-bheema-nn-0002:8020")
    sqlContext.setConf("dfs.nameservices", "bheema,athena-prod")
    sqlContext.setConf("dfs.ha.automatic-failover.enabled", "true")
    sqlContext
  }

  def getDirectoryPath(time: Long): String ={
    DateTimeFormat.forPattern("yyyy/MM/dd").print(time) + ""
  }
}
