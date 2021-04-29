
name := "lgem"

version := "0.1"

scalaVersion := "2.12.10"

idePackagePrefix := Some("mm.graph.embeddings")

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.2" //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.2" //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.2" //% "provided"
libraryDependencies += "com.typesafe" % "config" % "1.2.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % Test

mainClass in assembly := Some("mm.graph.embeddings.Main")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}