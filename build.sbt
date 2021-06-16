name := "gameround-settler"

version := "0.1"

scalaVersion := "2.12.13"

val sparkVersion = "3.1.1"
val kafkaVersion = "2.4.0"
val log4jVersion = "2.4.1"
val cassandraConnectorVersion = "3.0.1"
val airframeVersion = "21.4.1"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  // spark
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,

  "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnectorVersion,
  "com.github.jnr" % "jnr-posix" % "3.1.5",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.10.0",

  // kafka
  "org.apache.kafka" %% "kafka" % kafkaVersion,

  // logging
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,

  "com.github.scopt" %% "scopt" % "4.0.1",
  "org.wvlet.airframe" %% "airframe-config" % airframeVersion
)