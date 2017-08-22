import sbt.Keys._

name := "example-spark-scala-read-and-write-from-hive"

version := "1.1"

scalaVersion := "2.11.11"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.5.0"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
