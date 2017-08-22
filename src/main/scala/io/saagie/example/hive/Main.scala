package io.saagie.example.hive

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.OptionParser

object Main {

  val logger: Logger = LogManager.getLogger(getClass)

  case class CLIParams(hiveHost: String = "")

  // Defining an Helloworld class
  case class HelloWorld(message: String)

  def main(args: Array[String]): Unit = {

    val parser = parseArgs("Main")

    parser.parse(args, CLIParams()) match {
      case Some(params) =>
        // Creation of SparkSession
        val sparkSession = SparkSession.builder()
          .appName("example-spark-scala-read-and-write-from-hive")
          .config("hive.metastore.warehouse.dir", params.hiveHost + "user/hive/warehouse")
          .enableHiveSupport()
          .getOrCreate()

        // ====== Creating a dataframe with 1 partition
        import sparkSession.implicits._
        val df = Seq(HelloWorld("helloworld")).toDF().coalesce(1)

        // ======= Writing files
        // Writing Dataframe as a Hive table
        import sparkSession.sql

        sql("DROP TABLE IF EXISTS helloworld")
        sql("CREATE TABLE helloworld (message STRING)")
        df.write.mode(SaveMode.Overwrite).saveAsTable("helloworld")
        logger.info("Writing hive table : OK")

        // ======= Reading files
        // Reading hive table into a Spark Dataframe
        val dfHive = sql("SELECT * from helloworld")
        logger.info("Reading hive table : OK")
        logger.info(dfHive.show())

      case None =>
      // arguments are bad, error message will have been displayed
    }
  }

  def parseArgs(appName: String): OptionParser[CLIParams] = {
    new OptionParser[CLIParams](appName) {
      head(appName, "1.0")
      help("help") text "prints this usage text"

      opt[String]("hdfsHost") required() action { (data, conf) =>
        conf.copy(hiveHost = data)
      } text "hdfsHost. Example : hdfs://nn1.p2.prod.saagie.io:8020"
    }
  }
}
