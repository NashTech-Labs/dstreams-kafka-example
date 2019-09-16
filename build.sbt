organization := "knoldus"

name := "dstreams-kafka-example"

version := "0.1"

scalaVersion := "2.11.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "2.2.1",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.1",
  "net.manub" %% "scalatest-embedded-kafka" % "0.15.1" % Test,
  "org.scalatest" %% "scalatest" % "2.2.2" % Test
)
