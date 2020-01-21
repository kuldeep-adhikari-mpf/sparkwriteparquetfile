name := "sparkwriteparquetfile"

version := "0.1"

scalaVersion := "2.11.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.spark" %% "spark-mllib" % "2.4.4",
  "org.apache.spark" %% "spark-streaming" % "2.4.4",
  "org.scala-lang" % "scala-library" % "2.4.4" % "provided"

)