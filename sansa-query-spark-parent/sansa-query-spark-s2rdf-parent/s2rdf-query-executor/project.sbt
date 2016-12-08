name := "QueryExecutor"

version := "1.1"

scalacOptions += "-target:jvm-1.8"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.0"

