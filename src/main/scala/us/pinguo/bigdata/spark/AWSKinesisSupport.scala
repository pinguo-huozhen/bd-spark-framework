package us.pinguo.bigdata.spark

import java.nio.ByteBuffer

import com.amazonaws.auth.{BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.{PutRecordsRequest, PutRecordsRequestEntry, PutRecordsResultEntry, Record}
import com.typesafe.config.Config
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kinesis.KinesisUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._
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
    def saveToKinesis(streamName: String, region: Regions = Regions.US_EAST_1, awsAccessCredentials: Option[(String, String)] = None): RDD[PutRecordsResultEntry] = {
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
          client.putRecords(request).getRecords.toList
        }.flatten
      }
    }

  }

  implicit class SparkContextWithKinesis(ssc: StreamingContext) {

    def kinesisStream(applicationName: String, kinesisConfig: Config, initPositionType: InitialPositionInStream = InitialPositionInStream.TRIM_HORIZON): ReceiverInputDStream[Record] = {
      val kinesisStream = kinesisConfig.getString("stream")
      val kinesisEndpoint = kinesisConfig.getString("endpoint")
      val kinesisRegion = kinesisConfig.getString("region")
      val kinesisAccessKey = if (kinesisConfig.atKey("access-key").isEmpty) Some(kinesisConfig.getString("access-key")) else None
      val kinesisAccessSecret = if (kinesisConfig.atKey("access-secret").isEmpty) Some(kinesisConfig.getString("access-secret")) else None
      val kinesisIntervalCheckPoint = kinesisConfig.getLong("interval-check-point")

      if (kinesisAccessKey.isEmpty && kinesisAccessSecret.isEmpty) {
        KinesisUtils.createStream(
          ssc,
          applicationName, kinesisStream, kinesisEndpoint, kinesisRegion,
          initPositionType, Seconds(kinesisIntervalCheckPoint), StorageLevels.MEMORY_ONLY_SER_2,
          (record: Record) => record
        )
      } else {
        KinesisUtils.createStream(
          ssc,
          applicationName, kinesisStream, kinesisEndpoint, kinesisRegion,
          initPositionType, Seconds(kinesisIntervalCheckPoint), StorageLevels.MEMORY_ONLY_SER_2,
          (record: Record) => record,
          kinesisAccessKey.get, kinesisAccessSecret.get
        )
      }
    }
  }

}
