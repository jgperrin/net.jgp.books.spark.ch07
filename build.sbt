name := "SparkInAction2-Chapter07"

version := "1.0.0"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.0-preview2"
val sparkXmlVersion = "0.9.0"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,
  "com.databricks"   %% "spark-xml"  % sparkXmlVersion,
  "org.apache.spark" %% "spark-avro" % sparkVersion
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
