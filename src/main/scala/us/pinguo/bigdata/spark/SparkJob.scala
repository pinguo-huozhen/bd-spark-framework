package us.pinguo.bigdata.spark

import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

trait SparkJob {
  //default to china
  System.setProperty("user.timezone", "Asia/Shanghai")

  protected val logger: Logger = Logger.getLogger(this.getClass.getCanonicalName)

  protected def createSparkConf(applicationName:String, config: Config): SparkConf = {
    val conf = new SparkConf().setAppName(applicationName)
    conf.set("spark.hadoop.mapred.output.compress", "true")
    conf.set("spark.hadoop.mapred.output.compression.codec", "true")
    conf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
    conf.set("spark.hadoop.mapred.output.compression.type", "BLOCK")
    conf.set("spark.dynamicAllocation.enabled", "false")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer.max", "1024m")
    conf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35")
    if (config.hasPath("runtime")) {
      config.getConfig("runtime").entrySet() foreach { configValue =>
        conf.set(configValue.getKey, configValue.getValue.unwrapped().asInstanceOf[String])
      }
    }
    conf
  }

  protected def createContext(conf: SparkConf, awsId: String = "", awsKey: String = ""): SparkContext = {
    showCurrentConfiguration(conf)
    val context = new SparkContext(conf)
    setAwsS3Access(awsId, awsKey, context)
    context
  }

  private def setAwsS3Access(awsId: String, awsKey: String, context: SparkContext) = {
    context.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", awsId)
    context.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", awsKey)
  }

  protected def createStream(conf: SparkConf, awsId: String = "", awsKey: String = "", seconds: Duration = Seconds(30)): StreamingContext = {
    showCurrentConfiguration(conf)
    val context = new StreamingContext(conf, seconds)
    setAwsS3Access(awsId, awsKey, context.sparkContext)
    context
  }

  private def showCurrentConfiguration(conf: SparkConf) = {
    var submittedConf = "spark submitted with follow configuration:\n"
    conf.getAll foreach { case (k, v) =>
      submittedConf += s"$k -> $v\n"
    }
    logger.info(submittedConf)
  }
}