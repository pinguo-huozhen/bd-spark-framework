package us.pinguo.bigdata.spark.flow

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import us.pinguo.bigdata.spark.flow.SparkFlowBuilder.{SparkFlowFunc, _}

/**
  * Created by jim on 2017/2/4.
  */
object SparkFlowEngine {
  private var previousRDD: Any = _

  def runFlow[T](rdd: RDD[_], flow: List[SparkFlowFunc]): RDD[T] = {
    previousRDD = rdd
    flow.foreach(
      cmf => {
        process(cmf)
        if (cmf.useCache) {
          previousRDD.asInstanceOf[RDD[T]].persist(StorageLevel.MEMORY_AND_DISK_SER)
        }
      }
    )
    previousRDD.asInstanceOf[RDD[T]]
  }

  def process: PartialFunction[SparkFlowFunc, Any] = {
    case func: MapFunc => {

      previousRDD = previousRDD.asInstanceOf[RDD[_]].map(func.lambda)

    }
    case func: FilterFunc => {
      previousRDD = previousRDD.asInstanceOf[RDD[_]].filter(func.lambda)
      //previousRDD
    }
    case func: FlatMapFunc => {

      previousRDD = previousRDD.asInstanceOf[RDD[_]].flatMap(func.lambda)
    }
    case func: MapToKVFunc => {
      previousRDD = previousRDD.asInstanceOf[RDD[_]].map(func.lambda)
    }
    case func: MapPartitionWithIndexFunc => {
      previousRDD = previousRDD.asInstanceOf[RDD[_]].mapPartitionsWithIndex(func.lambda)
    }

    case func: MapPartitionFunc => {
      previousRDD = previousRDD.asInstanceOf[RDD[_]].mapPartitions(func.lambda)
    }

    case func: ForPartitionFunc => {
      previousRDD = {
        previousRDD.asInstanceOf[RDD[_]].foreachPartition(func.lambda)
        previousRDD
      }
    }

    case func: ReduceByKeyFunc => {
      if (func.mapFunc != null) {
        val rdd = previousRDD.asInstanceOf[RDD[Any]]
        val mappedRDD = {
          if (func.usePartitionTunning) {
            rdd.mapPartitions { iterator =>

              val mapped: Iterator[(String, Any)] = iterator.map(func.mapFunc)
              mapped.toSeq.groupBy(_._1).map(x => x._1 -> x._2.map(_._2).reduce(func.reduceByKeyFunc)).toIterator
            }
          } else {
            rdd.map(func.mapFunc)
          }
        }
        previousRDD = mappedRDD.reduceByKey(func.reduceByKeyFunc)
      } else {
        val rdd = previousRDD.asInstanceOf[RDD[(String, Any)]]
        previousRDD = rdd.reduceByKey(func.reduceByKeyFunc)
      }
    }


    case func: OutputFunc =>
      var outputRDD = previousRDD.asInstanceOf[RDD[_]]
      func.filterFunc collect {
        case filter => outputRDD = outputRDD.filter(filter)
      }
      func.mapFunc collect {
        case map => outputRDD = outputRDD.map(map)
      }
      func.outputFunc collect {
        case output => output(outputRDD)
      }

  }
}
