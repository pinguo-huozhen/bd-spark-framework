package us.pinguo.bigdata.spark

import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

trait SparkJob {
  //default to china
  System.setProperty("user.timezone", "Asia/Shanghai")

  protected val logger: Logger = Logger.getLogger(this.getClass.getCanonicalName)

  protected def createSparkConf(config: Config): SparkConf = {
    val conf = new SparkConf().setAppName(config.getString("application"))
    conf.set("spark.hadoop.mapred.output.compress", "true")
    conf.set("spark.hadoop.mapred.output.compression.codec", "true")
    conf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
    conf.set("spark.hadoop.mapred.output.compression.type", "BLOCK")
    conf.set("spark.dynamicAllocation.enabled", "false")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer", (1024 * 2).toString)
    conf.set("spark.sql.shuffle.partitions", "400")
    conf
  }

  protected def createContext(conf: SparkConf): SparkContext = {
    var submittedConf = "spark submitted with follow configuration:\n"
    conf.getAll foreach { case (k, v) =>
      submittedConf += s"$k -> $v\n"
    }
    logger.info(submittedConf)
    new SparkContext(conf)
  }

  protected def createStream(conf: SparkConf, seconds: Duration = Seconds(30)) = new StreamingContext(conf, seconds)
}