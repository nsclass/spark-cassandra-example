package com.nam.seo

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

/**
  * Date 2019-06-15 
  *
  * @author Nam Seob Seo
  */

object SparkCassandra {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SparkCassandra")
      .set("spark.cassandra.connection.host", "127.0.0.1")

    val sc = new SparkContext(conf)
    val rdd = sc.cassandraTable(keyspace = "system", table = "local")
        .select("key", "cluster_name", "cql_version")

    println("Data read as RDD")
    rdd.collect()
      .foreach(row => {
        println(row.getString("key"))
        println(row.getString("cluster_name"))
        println(row.getString("cql_version"))
      })
  }
}
