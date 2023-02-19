name := "odd-hunter"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.8",
  "org.apache.hadoop" % "hadoop-aws" % "2.6.5",
  "com.amazonaws" % "aws-java-sdk" % "1.11.698"
)
