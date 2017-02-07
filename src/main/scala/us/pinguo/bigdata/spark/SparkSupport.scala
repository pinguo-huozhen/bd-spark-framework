package us.pinguo.bigdata.spark

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._


trait SparkSupport {
  System.setProperty("user.timezone", "Asia/Shanghai")

  protected val logger: Logger = Logger.getLogger(this.getClass.getCanonicalName)

  protected def createSparkConf(config: Config): (SparkConf, Config) = {

    val finalConfig = config.withFallback(ConfigFactory.parseResources("default-spark.conf")).resolve()

    val sparkConf = new SparkConf().setAppName(finalConfig.getString("application"))

    finalConfig.getConfig("runtime").entrySet() foreach { configValue =>
      sparkConf.set(configValue.getKey, configValue.getValue.unwrapped().asInstanceOf[String])
    }

    (sparkConf, finalConfig)
  }

  protected implicit class SparkStreaming(context: SparkContext) {
    def streaming(duration: Duration) = new StreamingContext(context, duration)
  }

  protected def createSpark(config: Config): (SparkSession, SparkContext) = {
    val (sparkConfig, finalConfig) = createSparkConf(config)
    val session = SparkSession.builder().config(sparkConfig).getOrCreate()
    val context = session.sparkContext
    if (finalConfig.getString("hadoop.fs.s3n.awsAccessKeyId").nonEmpty) {
      context.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", finalConfig.getString("hadoop.fs.s3n.awsAccessKeyId"))
      context.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", finalConfig.getString("hadoop.fs.s3n.awsSecretAccessKey"))
    }
    (session, context)
  }
}