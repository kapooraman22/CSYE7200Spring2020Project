
name := "CSYE7200Spring2020Project-master"

version := "0.1"

scalaVersion := "2.11.11"
val sparkVersion = "2.3.1"

libraryDependencies ++= Seq(
  "org.lz4" % "lz4-java" % "1.4.0",
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier   "tests",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "org.typelevel" %% "cats-core" % "1.1.0",
  "org.typelevel" %% "cats-effect" % "1.0.0-RC2",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % Provided exclude ("net.jpountz.lz4", "lz4"),
  "com.pusher" % "pusher-java-client" % "1.8.0",
)
// https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-databind
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.6"
// https://mvnrepository.com/artifact/com.fasterxml.jackson.module/jackson-module-scala
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.5"
// https://mvnrepository.com/artifact/mysql/mysql-connector-java
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.13"
// https://mvnrepository.com/artifact/com.ibm.icu/icu4j
libraryDependencies += "com.ibm.icu" % "icu4j" % "4.8"
// https://mvnrepository.com/artifact/com.cedarsoftware/java-util
libraryDependencies += "com.cedarsoftware" % "java-util" % "1.8.0"
// https://mvnrepository.com/artifact/com.typesafe.akka/akka-http
libraryDependencies += "com.typesafe.akka" %% "akka-http" % "10.1.3"
// https://mvnrepository.com/artifact/com.typesafe.akka/akka-stream
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.13"
// https://mvnrepository.com/artifact/com.typesafe.akka/akka-actor
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.13"
// https://mvnrepository.com/artifact/com.cloudera.sparkts/sparkts
libraryDependencies += "com.cloudera.sparkts" % "sparkts" % "0.4.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.1"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"
// Akka http testkit
libraryDependencies += "com.typesafe.akka" %% "akka-http-testkit" % "10.1.3"

scalacOptions += "-Ypartial-unification"

// Avoids SI-3623
target := file("/tmp/sbt/CSYE7200Spring2020Project-master")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
test in assembly := {}
mainClass in assembly := Some("coinyser.BatchProducerAppSpark")