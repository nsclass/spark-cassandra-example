name := "spark-cassandra-test"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.3"
val cassandraConnectorVersion = "2.4.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnectorVersion
