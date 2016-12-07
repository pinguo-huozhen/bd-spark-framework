package us.pinguo.bigdata.spark

import java.nio.ByteBuffer

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.{PutRecordsRequest, PutRecordsRequestEntry, PutRecordsResultEntry}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

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
        val request = new PutRecordsRequest()
        request.setStreamName(streamName)
        request.setRecords(
          rows.map { case (partitionKey, raw) =>
            val entry = new PutRecordsRequestEntry()
            entry.setData(ByteBuffer.wrap(raw))
            entry.setPartitionKey(partitionKey)
            entry
          }.toList.asJava
        )
        client.putRecords(request).getRecords.toIterator
      }
    }

  }

}
