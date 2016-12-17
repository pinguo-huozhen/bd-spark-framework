resolvers += "Artifactory" at "http://54.222.244.187:8081/artifactory/bigdata/"

publishTo := Some("Artifactory Realm" at "http://54.222.244.187:8081/artifactory/bigdata;build.timestamp=" + new java.util.Date().getTime)

credentials += Credentials(Path.userHome / ".sbt" / "credentials")

lazy val `spark-framework` = (project in file("."))
  .settings(
    name := "spark-framework",
    organization := "us.pinguo.bigdata",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.11.8",
    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % "1.3.1",
      "org.apache.spark" %% "spark-yarn" % "2.0.2",
      "org.apache.spark" %% "spark-core" % "2.0.2",
      "org.apache.spark" %% "spark-sql" % "2.0.2",
      "org.apache.spark" %% "spark-mllib" % "2.0.2",
      "org.apache.spark" %% "spark-streaming-kinesis-asl" % "2.0.2",
      "org.elasticsearch" %% "elasticsearch-spark-20" % "5.0.0"
    )
  )