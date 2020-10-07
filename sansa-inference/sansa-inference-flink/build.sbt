organization := "net.sansa-stack"

name := "sansa-inference-flink"

val projectVersion = "0.1.0-SNAPSHOT"
val flinkVersion = "1.1.3"
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
  "org.apache.flink" %%  "flink-scala" % flinkVersion,
  "org.apache.flink" %%  "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %%  "flink-clients" % flinkVersion,
  "org.apache.flink" %%  "flink-test-utils" % flinkVersion % "test",
  "org.apache.jena" %  "jena-core" % jenaVersion,
  "org.apache.jena" %  "jena-arq" % jenaVersion,
  "com.typesafe.scala-logging" %%  "scala-logging" % "3.5.0",
  "com.google.guava" %  "guava" % "19.0",
  "com.github.scopt" %%  "scopt" % "3.2.0",
  "org.scalatest"     %% "scalatest"   % "3.0.1" % "test" withSources(),
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
