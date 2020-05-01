
name := "sparkwriteparquetfile"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "Java Maven2 Repository" at "https://artifactory-repo-1-na-prod.mplatform.com/artifactory/repo/"


libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % "2.3.0").
    exclude("com.google.protobuf", "protobuf-java") ,
  ("org.apache.spark" %% "spark-sql" % "2.3.0").
    exclude("com.google.protobuf", "protobuf-java") ,
  "org.apache.spark" %% "spark-mllib" % "2.3.0",
//  "org.apache.spark" %% "spark-streaming" % "2.4.4",
 // "com.mplatform.mi-wrs" % "data-collect-hbase" % "6.2.6-172-SNAPSHOT",
    // https://mvnrepository.com/artifact/org.apache.spark/spark-yarn
  ("org.apache.spark" %% "spark-yarn" % "2.3.0").
    exclude("com.google.protobuf", "protobuf-java"),
  "com.google.protobuf" % "protobuf-java" % "3.5.1",

  // https://mvnrepository.com/artifact/com.google.cloud.bigdataoss/gcs-connector
// "com.google.cloud.bigdataoss" % "gcs-connector" % "1.3.2-hadoop2"
"org.xolstice.maven.plugins" % "protobuf-maven-plugin" % "0.6.1"
)

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.protobuf" -> "shaded.com.google.protobuf").inAll,
  ShadeRule.rename("com.google.common" -> "shaded.com.google.common").inAll,
  ShadeRule.rename("org.apache.hadoop.tools" -> "shaded.org.apache.hadoop.tools").inAll
)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.rename
}