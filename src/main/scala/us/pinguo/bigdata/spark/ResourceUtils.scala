package us.pinguo.bigdata.spark

import scala.io.Source

object ResourceUtils {
  def resourceAsString(name: String): String = {
    Source.fromInputStream(getClass.getResourceAsStream(s"/$name")).mkString
  }
}
