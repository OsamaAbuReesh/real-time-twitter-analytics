name := "RealTimeTwitterAnalytics"

version := "0.1"

scalaVersion := "2.13.12"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "3.5.1",
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-streaming" % "3.5.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.5.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "4.5.4" ,
  "edu.stanford.nlp" % "stanford-corenlp" % "4.5.4" classifier "models",
  "com.softwaremill.sttp.client3" %% "core" % "3.8.15",
  "com.softwaremill.sttp.client3" %% "sttp-client3" % "3.3.10"

)
