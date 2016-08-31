package io.saagie.example.hive

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import scopt.OptionParser

object Main{

  val logger = LogManager.getLogger(this.getClass())

  case class CLIParams(hiveHost: String = "")

  // Defining an Helloworld class
  case class HelloWorld(message: String)

  def main(args: Array[String]): Unit = {

    val parser = parseArgs("Main")

    parser.parse(args, CLIParams()) match {
      case Some(params) =>

    // Configuration of SparkContext
    val conf = new SparkConf().setAppName("example-spark-scala-read-and-write-from-hive")

    // Creation of SparContext and SQLContext
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    // Configuring hiveContext to find hive metastore
    hiveContext.setConf("hive.metastore.warehouse.dir", params.hiveHost + "user/hive/warehouse")

    // ====== Creating a dataframe with 1 partition
    import hiveContext.implicits._
    val df = Seq(HelloWorld("helloworld")).toDF().coalesce(1)

    // ======= Writing files
    // Writing Dataframe as a Hive table
    hiveContext.sql("DROP TABLE IF EXISTS helloworld")
    hiveContext.sql("CREATE TABLE helloworld (message STRING)")
    df.write.mode("overwrite").saveAsTable("helloworld");
    logger.info("Writing hive table : OK")

    // ======= Reading files
    // Reading hive table into a Spark Dataframe
    val dfHive = hiveContext.sql("SELECT * from helloworld")
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
