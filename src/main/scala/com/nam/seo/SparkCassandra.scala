package com.nam.seo

import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.{SparkSession}

/**
  * Date 2019-06-15 
  *
  * @author Nam Seob Seo
  */

object SparkCassandra {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkCassandra")
      .master("local")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .getOrCreate()

    // Read data as RDD
    val rdd = spark.sparkContext.cassandraTable(keyspace = "system", table = "local")
      .select("key", "cluster_name", "cql_version")

    println("Data read as RDD")
    rdd.collect()
      .foreach(row => {
        println(row.getString("key"))
        println(row.getString("cluster_name"))
        println(row.getString("cql_version"))
      })

    // Read data as DataSet (DataFrame)
    val dataset = spark.read
      .cassandraFormat(keyspace = "system", table = "local")
      .load()

    dataset.collect()
      .foreach(row => {
        println(row.getAs("key"))
        println(row.getAs("cluster_name"))
        println(row.getAs("cql_version"))
      })
  }
}
