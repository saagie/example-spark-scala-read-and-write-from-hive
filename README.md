Example for readind & writing into hive with Spark Scala
========================================================

Package for saagie : sbt clean assembly and get the package in target.

Usage in local :

 - sbt clean assembly
 - spark-submit --class=io.saagie.example.hdfs.Main example-spark-scala-read-and-write-from-hive-assembly-1.1.jar --hdfsHost "hdfs://hdfshost:8020/"

Usage in Saagie :

 - sbt clean assembly (in local, to generate jar file)
 - create new Spark Job
 - upload the jar (target/scala-2.11/example-spark-scala-read-and-write-from-hive-assembly-1.1.jar)
 - Replace MyClass with the full class name (ex : io.saagie.example.hive.Main)
 - copy URL from HDFS connection details panel and put it as argument after --hdfsHost
 - choose Spark 2.1.0
 - choose resources you want to allocate to the job
 - create and launch.
