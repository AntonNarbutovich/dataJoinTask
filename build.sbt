name := "soleadifyTask"

version := "0.1"

scalaVersion := "2.13.12"
val sparkVersion = "3.5.0"


libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
