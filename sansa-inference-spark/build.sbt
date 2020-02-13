organization := "net.sansa-stack"

name := "sansa-inference-spark"

val projectVersion = "0.7.2-SNAPSHOT"
val sparkVersion = "2.4.4"
val jenaVersion = "3.13.1"
val owlapiVersion = "5.1.12"

scalaVersion := "2.11.12"

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

scalacOptions ++= Seq("-Xmax-classfile-name","128")

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
  "net.sansa-stack" %%  "sansa-inference-common" % projectVersion,
  "net.sansa-stack" %%  "sansa-inference-tests" % projectVersion % "test" classifier "tests",
  "net.sansa-stack" %%  "sansa-rdf-spark" % projectVersion,
  "net.sansa-stack" %%  "sansa-query-spark" % projectVersion,
  "net.sansa-stack" %%  "sansa-owl-spark" % projectVersion,
  "org.aksw.jena-sparql-api" %  "jena-sparql-api-server-standalone" % "3.13.1-1-SNAPSHOT",
  "org.scala-lang" %  "scala-library" % "2.11.12",
  "org.apache.spark" %%  "spark-core" % sparkVersion,
  "org.apache.spark" %%  "spark-sql" % sparkVersion,
  "com.chuusai" %%  "shapeless" % "2.3.0",
  "org.apache.jena" %  "jena-core" % jenaVersion,
  "org.apache.jena" %  "jena-arq" % jenaVersion,
  "net.sourceforge.owlapi" %  "owlapi-api" % owlapiVersion,
  "net.sourceforge.owlapi" %  "owlapi-apibinding" % owlapiVersion,
  "net.sourceforge.owlapi" %  "owlapi-impl" % owlapiVersion,
  "net.sourceforge.owlapi" %  "owlapi-parsers" % owlapiVersion,
  "com.assembla.scala-incubator" %%  "graph-core" % "1.11.0",
  "com.assembla.scala-incubator" %%  "graph-dot" % "1.11.0",
  "org.jgrapht" %  "jgrapht-core" % "1.2.0",
  "org.jgrapht" %  "jgrapht-ext" % "1.2.0",
  "org.gephi" %  "gephi-toolkit" % "0.9.1",
  "com.holdenkarau" %%  "spark-testing-base" % "2.4.3_0.12.0" % "test",
  "com.typesafe.scala-logging" %%  "scala-logging" % "3.5.0",
  "com.github.scopt" %%  "scopt" % "3.7.0",
  "org.scalatest"     %% "scalatest"   % "3.0.5" % "test" withSources(),
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
