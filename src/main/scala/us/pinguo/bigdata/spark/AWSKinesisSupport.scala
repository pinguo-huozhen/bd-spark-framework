package us.pinguo.bigdata.spark

import java.nio.ByteBuffer

import com.amazonaws.auth.{BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model._
import com.typesafe.config.Config
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kinesis.KinesisUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConverters._

object AWSKinesisSupport {

  implicit class RDDWithKinesisSupport(rdd: RDD[(String, Array[Byte])]) {

    /**
      * try put result to kinesis, this is a transform not action, should process record put result and all failed should be handled
      *
      * @param credentials aws credentials be used
      * @param region      kinesis stream aws region
      * @param streamName  kinesis stream name
      * @return a RDD with all PutRecordsResultEntry object
      */
    def saveToKinesis(streamName: String, region: Regions = Regions.US_EAST_1, awsAccessCredentials: Option[(String, String)] = None, batchSize: Int = 200): RDD[PutRecordsResult] = {
      rdd.mapPartitions { rows =>
        val client = awsAccessCredentials match {
          case Some(accessCredentials) => new AmazonKinesisClient(new BasicAWSCredentials(accessCredentials._1, accessCredentials._2))
          case None => new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain())
        }
        client.setRegion(Region.getRegion(region))

        rows.map { case (partitionKey, raw) =>
          val entry = new PutRecordsRequestEntry()
          entry.setData(ByteBuffer.wrap(raw))
          entry.setPartitionKey(partitionKey)
          entry
        }.toList.grouped(100).map { groupedRecords =>
          val request = new PutRecordsRequest()
          request.setStreamName(streamName)
          request.setRecords(groupedRecords.asJava)
          client.putRecords(request)
        }
      }
    }

  }

  implicit class SparkContextWithKinesis(ssc: StreamingContext) {

    def kinesisStream(applicationName: String, kc: Config): DStream[Record] = {
      val kinesisStream = kc.getString("stream")
      val kinesisEndpoint = kc.getString("endpoint")
      val kinesisRegion = kc.getString("region")
      val kinesisAccessKey = if (kc.hasPath("access-key")) Some(kc.getString("access-key")) else None
      val kinesisAccessSecret = if (kc.hasPath("access-secret")) Some(kc.getString("access-secret")) else None
      val kinesisReceivers = if (kc.hasPath("num-of-partitions")) Some(kc.getInt("num-of-partitions")) else None
      val kinesisInitialPositionInStream =
        if (kc.hasPath("initial-position-in-stream")) InitialPositionInStream.valueOf(kc.getString("initial-position-in-stream")) else InitialPositionInStream.TRIM_HORIZON

      val kinesisIntervalCheckPoint = kc.getLong("interval-check-point")

      val streams = 0 until kinesisReceivers.getOrElse(1) map { _ =>
        if (kinesisAccessKey.isEmpty && kinesisAccessSecret.isEmpty) {
          KinesisUtils.createStream(
            ssc,
            applicationName, kinesisStream, kinesisEndpoint, kinesisRegion,
            kinesisInitialPositionInStream, Seconds(kinesisIntervalCheckPoint), StorageLevels.MEMORY_ONLY_SER_2,
            (record: Record) => record
          )
        } else {
          KinesisUtils.createStream(
            ssc,
            applicationName, kinesisStream, kinesisEndpoint, kinesisRegion,
            kinesisInitialPositionInStream, Seconds(kinesisIntervalCheckPoint), StorageLevels.MEMORY_ONLY_SER_2,
            (record: Record) => record,
            kinesisAccessKey.get, kinesisAccessSecret.get
          )
        }
      }
      ssc.union(streams)
    }
  }

}
