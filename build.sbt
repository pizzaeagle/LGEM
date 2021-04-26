
name := "lgem"

version := "0.1"

scalaVersion := "2.11.12"

idePackagePrefix := Some("mm.graph.embeddings")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.7" % "provided"
libraryDependencies += "com.typesafe" % "config" % "1.2.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % Test

mainClass in assembly := Some("mm.graph.embeddings.Main")
