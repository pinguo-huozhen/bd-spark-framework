package us.pinguo.bigdata.spark.flow

import us.pinguo.bigdata.spark.flow.SparkFlowBuilder.SparkFlowFunc

/**
  * Created by jim on 2017/2/4.
  */

class SparkFlowBuilder {

  var flows = List[SparkFlowFunc]()

  def append(func: SparkFlowFunc): SparkFlowBuilder = {
    flows ::= func
    this
  }

  def flow = flows.reverse

}
object SparkFlowBuilder {

  trait SparkFlowFunc {
    var useCache: Boolean = false

    def setUseche(isCache: Boolean = false) = {
      useCache = isCache
      this
    }
  }

  case class MapFunc(lambda: Any => Any) extends SparkFlowFunc

  case class MapPartitionWithIndexFunc(lambda: (Int, Iterator[Any]) => Iterator[Any]) extends SparkFlowFunc

  case class MapPartitionFunc(lambda: (Iterator[Any]) => Iterator[Any]) extends SparkFlowFunc

  case class FlatMapFunc(lambda: Any => Seq[Any]) extends SparkFlowFunc

  case class MapToKVFunc(lambda: Any => (String, Any)) extends SparkFlowFunc

  case class FilterFunc(lambda: Any => Boolean) extends SparkFlowFunc

  case class ForPartitionFunc(lambda: (Iterator[Any]) => Unit) extends SparkFlowFunc

  case class ReduceByKeyFunc(usePartitionTunning: Boolean = false) extends SparkFlowFunc {
    var mapFunc: Any => (String, Any) = _
    var reduceByKeyFunc: (Any, Any) => Any = _
    //var filterFunc: Option[Any => Boolean] = None

    def map(f: Any => (String, Any)) = {
      mapFunc = f
      this
    }

    def reduce(f: (Any, Any) => Any) = {
      reduceByKeyFunc = f
      this
    }
  }


  case class OutputFunc(to: String) extends SparkFlowFunc {
    var mapFunc: Option[Any => String] = None
    var filterFunc: Option[Any => Boolean] = None

    def map(f: Any => String) = {
      mapFunc = Some(f)
      this
    }

    def filter(f: Any => Boolean) = {
      filterFunc = Some(f)
      this
    }
  }
}
