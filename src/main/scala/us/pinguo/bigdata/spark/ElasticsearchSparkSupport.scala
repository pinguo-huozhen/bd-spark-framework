package us.pinguo.bigdata.spark

import com.typesafe.config.Config

object ElasticsearchSparkSupport {

  implicit class TypeConfigAsElasticsearchConfig(config: Config) {

    def asESConf: Map[String, String] = {
      val conf = Map(
        "es.nodes" -> config.getString("nodes"),
        "es.resource" -> config.getString("resource"),
        "es.write.operation" -> "upsert",
        "es.http.timeout" -> "30s",
        "es.mapping.id" -> config.getString("mapping-id")
      )
      if (config.getBoolean("remote")) {
        conf ++ Map("es.nodes.discovery" -> "false", "es.nodes.wan.only" -> "true")
      } else {
        conf
      }
    }
  }

}
