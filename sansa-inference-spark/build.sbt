organization := "net.sansa-stack"

name := "sansa-inference-spark"

val projectVersion = "0.1.0-SNAPSHOT"
val sparkVersion = "2.0.2"
val jenaVersion = "3.1.1"

scalaVersion := "2.11.8"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-target:jvm-1.8",
  "-unchecked",
  "-Ywarn-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-unused",
  "-Ywarn-value-discard",
  "-Xfuture",
  "-Xlint"
)

javacOptions ++= Seq(
  "-Xlint:deprecation",
  "-Xlint:unchecked",
  "-source", "1.8",
  "-target", "1.8",
  "-g:vars"
)

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  "AKSW Snapshots" at "http://maven.aksw.org/archiva/repository/snapshots",
  Resolver.mavenLocal
)


libraryDependencies ++= Seq(
  "net.sansa-stack" %  "sansa-inference-common" % projectVersion,
  "net.sansa-stack" %  "sansa-inference-tests" % projectVersion % "test",
  "net.sansa-stack" %  "sansa-rdf-spark-core" % projectVersion,
  "net.sansa-stack" %  "sansa-rdf-partition-core" % projectVersion,
  "net.sansa-stack" %  "sansa-query-spark-sparqlify" % projectVersion,
  "org.aksw.jena-sparql-api" %  "jena-sparql-api-server-standalone" % "3.1.1-1-SNAPSHOT",
  "org.scala-lang" %  "scala-library" % "2.11.8",
  "org.apache.spark" %%  "spark-core" % sparkVersion,
  "org.apache.spark" %%  "spark-sql" % sparkVersion,
  "com.chuusai" %%  "shapeless" % "2.3.0",
  "org.apache.jena" %  "jena-core" % jenaVersion,
  "org.apache.jena" %  "jena-arq" % jenaVersion,
  "com.assembla.scala-incubator" %%  "graph-core" % "1.11.0",
  "com.assembla.scala-incubator" %%  "graph-dot" % "1.11.0",
  "org.jgrapht" %  "jgrapht-core" % "1.0.0",
  "org.jgrapht" %  "jgrapht-ext" % "1.0.0",
  "org.gephi" %  "gephi-toolkit" % "0.9.1",
  "junit" %  "junit" % "4.12",
  "org.scalatest" %%  "scalatest" % "3.0.1",
  "com.holdenkarau" %%  "spark-testing-base" % "2.0.0_0.4.4" % "test",
  "com.typesafe.scala-logging" %%  "scala-logging" % "3.5.0",
  "com.github.scopt" %%  "scopt" % "3.5.0",
  //"com.github.nscala-time"  %% "nscala-time"   % "1.8.0" withSources(),
  "org.scalatest"     %% "scalatest"   % "3.0.0" % "test" withSources(),
  "junit"             %  "junit"       % "4.12"  % "test"
)

logLevel := Level.Warn

// Only show warnings and errors on the screen for compilations.
// This applies to both test:compile and compile and is Info by default
logLevel in compile := Level.Warn

// Level.INFO is needed to see detailed output when running tests
logLevel in test := Level.Info

// define the statements initially evaluated when entering 'console', 'console-quick', but not 'console-project'
initialCommands in console := """
                                |""".stripMargin

cancelable := true
