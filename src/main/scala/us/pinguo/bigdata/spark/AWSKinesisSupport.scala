package us.pinguo.bigdata.spark

import java.nio.ByteBuffer

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.{PutRecordsRequest, PutRecordsRequestEntry, PutRecordsResultEntry}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

object AWSKinesisSupport {

  implicit class RDDWithKinesisSupport[T](rdd: RDD[(String, Array[Byte])]) {
    /**
      * try put result to kinesis, this is a transform not action, should process record put result and all failed should be handled
      *
      * @param awsAccessKey    aws account be used
      * @param awsAccessSecret aws account be used
      * @param region          kinesis stream aws region
      * @param streamName      kinesis stream name
      * @return a RDD with all PutRecordsResultEntry object
      */
    def saveToKinesis(awsAccessKey: String, awsAccessSecret: String, region: Regions, streamName: String): RDD[PutRecordsResultEntry] = {
      rdd.mapPartitions { rows =>
        val client = new AmazonKinesisClient(new BasicAWSCredentials(awsAccessKey, awsAccessSecret))
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
