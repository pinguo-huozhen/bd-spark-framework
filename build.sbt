lazy val `spark-framework` = (project in file("."))
  .settings(
    name := "spark-framework",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.11.8",
    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % "1.3.1",
      "org.apache.spark" %% "spark-yarn" % "2.0.1",
      "org.apache.spark" %% "spark-core" % "2.0.1",
      "org.apache.spark" %% "spark-sql" % "2.0.1",
      "org.apache.spark" %% "spark-mllib" % "2.0.1",
      "org.apache.spark" %% "spark-streaming-kinesis-asl" % "2.0.1"
    )
  )