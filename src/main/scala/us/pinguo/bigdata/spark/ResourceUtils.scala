package us.pinguo.bigdata.spark

import scala.io.Source

object ResourceUtils {
  implicit class class2Resource(clazz:Class[_]) {
    def resourceAsString(name: String): String = {
      Source.fromURL(clazz.getResource(s"/$name")).mkString
    }
  }

}
