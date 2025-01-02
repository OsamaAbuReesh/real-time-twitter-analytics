name := "RealTimeTwitterAnalytics"

version := "0.1"
scalaVersion := "2.13.12"

libraryDependencies ++= Seq(
  // Kafka dependencies
  "org.apache.kafka" % "kafka-clients" % "3.5.1",

  // Spark dependencies
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-streaming" % "3.5.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.5.0",

  // STTP Client dependency
  "com.softwaremill.sttp.client3" %% "core" % "3.8.15",

  // Circe dependencies for JSON parsing
  "io.circe" %% "circe-core" % "0.14.3",
  "io.circe" %% "circe-generic" % "0.14.3",
  "io.circe" %% "circe-parser" % "0.14.3",
  "io.circe" %% "circe-literal" % "0.14.3",

  // JSON handling dependencies
  "org.json4s" %% "json4s-native" % "4.0.3",
  "org.json4s" %% "json4s-jackson" % "4.0.6",

  // GeoTools dependencies
  "org.geotools" % "gt-shapefile" % "28.0" exclude ("javax.media", "jai_core"),
  "org.geotools" % "gt-referencing" % "28.0" exclude ("javax.media", "jai_core"),
  "org.geotools" % "gt-geojson" % "28.0" exclude ("javax.media", "jai_core"),
  "org.locationtech.jts" % "jts-core" % "1.19.0",

  // Apache HTTP Components
  "org.apache.httpcomponents" % "httpclient" % "4.5.13",
  "org.apache.httpcomponents" % "httpcore" % "4.4.15",

  // Elasticsearch Java Client
  "co.elastic.clients" % "elasticsearch-java" % "8.9.0",
  // Logging dependencies (Optional)
  "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
  "org.apache.logging.log4j" % "log4j-api" % "2.20.0",

)

resolvers ++= Seq(
  "Maven Central" at "https://repo1.maven.org/maven2/",
  "OSGeo" at "https://repo.osgeo.org/repository/release/",
  "Elastic" at "https://artifacts.elastic.co/maven"
)
