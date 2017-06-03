package us.pinguo.bigdata.spark

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

/**
  * Created by huozhen on 2017/6/3.
  */
class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    val keyStr = key.asInstanceOf[String]
    s"$keyStr/$name"
  }


  override protected def generateActualKey(key: Any, value: Any): String = null

}
