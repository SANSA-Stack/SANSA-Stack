/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io._
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.Locale

import scala.io.Source
import scala.util.Properties
import scala.jdk.CollectionConverters._
import scala.collection.mutable.Stack

import sbt._
import sbt.Classpaths.publishTask
import sbt.Keys._
import sbt.librarymanagement.{ VersionNumber, SemanticSelector }
import com.etsy.sbt.checkstyle.CheckstylePlugin.autoImport._
import com.simplytyped.Antlr4Plugin._
import com.typesafe.sbt.pom.{PomBuild, SbtPomKeys}
import com.typesafe.tools.mima.plugin.MimaKeys
import org.scalastyle.sbt.ScalastylePlugin.autoImport._
import org.scalastyle.sbt.Tasks
import sbtassembly.AssemblyPlugin.autoImport._

import spray.revolver.RevolverPlugin._

object BuildCommons {

  private val buildLocation = file(".").getAbsoluteFile.getParentFile

  val allProjects@Seq(
    rdf, query, owl, datalake, inference, ml, _*
  ) = Seq(
    "sansa-rdf", "sansa-query", "sansa-owl", "sansa-datalake", "sansa-inference", "snasa-ml"
  ).map(ProjectRef(buildLocation, _))

  val tools = ProjectRef(buildLocation, "tools")
  // Root project.
  val sansa = ProjectRef(buildLocation, "SANSA-Stack")
  val sansaHome = buildLocation

  val testTempDir = s"$sansaHome/target/tmp"

  val javaVersion = settingKey[String]("source and target JVM version for javac and scalac")
}

object SANSABuild extends PomBuild {

  import BuildCommons._
  import sbtunidoc.GenJavadocPlugin
  import sbtunidoc.GenJavadocPlugin.autoImport._
  import scala.collection.mutable.Map

  val projectsMap: Map[String, Seq[Setting[_]]] = Map.empty

  override val profiles = {
    val profiles = Properties.envOrNone("SBT_MAVEN_PROFILES")
      .orElse(Properties.propOrNone("sbt.maven.profiles")) match {
      case None => Seq("sbt")
      case Some(v) =>
        v.split("(\\s+|,)").filterNot(_.isEmpty).map(_.trim.replaceAll("-P", "")).toSeq
    }
    profiles
  }

  Properties.envOrNone("SBT_MAVEN_PROPERTIES") match {
    case Some(v) =>
      v.split("(\\s+|,)").filterNot(_.isEmpty).map(_.split("=")).foreach(x => System.setProperty(x(0), x(1)))
    case _ =>
  }

  override val userPropertiesMap = System.getProperties.asScala.toMap

  lazy val MavenCompile = config("m2r") extend(Compile)
  lazy val SbtCompile = config("sbt") extend(Compile)

  lazy val sparkGenjavadocSettings: Seq[sbt.Def.Setting[_]] = GenJavadocPlugin.projectSettings ++ Seq(
    scalacOptions ++= Seq(
      "-P:genjavadoc:strictVisibility=true" // hide package private types
    )
  )

  lazy val scalaStyleRules = Project("scalaStyleRules", file("scalastyle"))
    .settings(
      libraryDependencies += "org.scalastyle" %% "scalastyle" % "1.0.0"
    )

  lazy val scalaStyleOnCompile = taskKey[Unit]("scalaStyleOnCompile")

  lazy val scalaStyleOnTest = taskKey[Unit]("scalaStyleOnTest")

  // We special case the 'println' lint rule to only be a warning on compile, because adding
  // printlns for debugging is a common use case and is easy to remember to remove.
  val scalaStyleOnCompileConfig: String = {
    val in = "scalastyle-config.xml"
    val out = "scalastyle-on-compile.generated.xml"
    val replacements = Map(
      """customId="println" level="error"""" -> """customId="println" level="warn""""
    )
    var contents = Source.fromFile(in).getLines.mkString("\n")
    for ((k, v) <- replacements) {
      require(contents.contains(k), s"Could not rewrite '$k' in original scalastyle config.")
      contents = contents.replace(k, v)
    }
    new PrintWriter(out) {
      write(contents)
      close()
    }
    out
  }

  // Return a cached scalastyle task for a given configuration (usually Compile or Test)
  private def cachedScalaStyle(config: Configuration) = Def.task {
    val logger = streams.value.log
    // We need a different cache dir per Configuration, otherwise they collide
    val cacheDir = target.value / s"scalastyle-cache-${config.name}"
    val cachedFun = FileFunction.cached(cacheDir, FilesInfo.lastModified, FilesInfo.exists) {
      (inFiles: Set[File]) => {
        val args: Seq[String] = Seq.empty
        val scalaSourceV = Seq(file(scalaSource.in(config).value.getAbsolutePath))
        val configV = (baseDirectory in ThisBuild).value / scalaStyleOnCompileConfig
        val configUrlV = scalastyleConfigUrl.in(config).value
        val streamsV = (streams.in(config).value: @sbtUnchecked)
        val failOnErrorV = true
        val failOnWarningV = false
        val scalastyleTargetV = scalastyleTarget.in(config).value
        val configRefreshHoursV = scalastyleConfigRefreshHours.in(config).value
        val targetV = target.in(config).value
        val configCacheFileV = scalastyleConfigUrlCacheFile.in(config).value

        logger.info(s"Running scalastyle on ${name.value} in ${config.name}")
        Tasks.doScalastyle(args, configV, configUrlV, failOnErrorV, failOnWarningV, scalaSourceV,
          scalastyleTargetV, streamsV, configRefreshHoursV, targetV, configCacheFileV)

        Set.empty
      }
    }

    cachedFun(findFiles(scalaSource.in(config).value))
  }

  private def findFiles(file: File): Set[File] = if (file.isDirectory) {
    file.listFiles().toSet.flatMap(findFiles) + file
  } else {
    Set(file)
  }

  def enableScalaStyle: Seq[sbt.Def.Setting[_]] = Seq(
    scalaStyleOnCompile := cachedScalaStyle(Compile).value,
    scalaStyleOnTest := cachedScalaStyle(Test).value,
    logLevel in scalaStyleOnCompile := Level.Warn,
    logLevel in scalaStyleOnTest := Level.Warn,
    (compile in Compile) := {
      scalaStyleOnCompile.value
      (compile in Compile).value
    },
    (compile in Test) := {
      scalaStyleOnTest.value
      (compile in Test).value
    }
  )

  // Silencer: Scala compiler plugin for warning suppression
  // Aim: enable fatal warnings, but suppress ones related to using of deprecated APIs
  // depends on scala version:
  // <2.13 - silencer 1.6.0 and compiler settings to enable fatal warnings
  // 2.13.0,2.13.1 - silencer 1.7.1 and compiler settings to enable fatal warnings
  // 2.13.2+ - no silencer and configured warnings to achieve the same
  lazy val compilerWarningSettings: Seq[sbt.Def.Setting[_]] = Seq(
    libraryDependencies ++= {
//      if (VersionNumber(scalaVersion.value).matchesSemVer(SemanticSelector("<2.13.2"))) {
//        val silencerVersion = if (scalaBinaryVersion.value == "2.13") "1.7.1" else "1.6.0"
//        Seq(
//          "org.scala-lang.modules" %% "scala-collection-compat" % "2.2.0",
//          compilerPlugin("com.github.ghik" % "silencer-plugin" % silencerVersion cross CrossVersion.full),
//          "com.github.ghik" % "silencer-lib" % silencerVersion % Provided cross CrossVersion.full
//        )
//      } else {
//        Seq.empty
//      }
      Seq.empty
    },
    scalacOptions in Compile ++= {
      if (VersionNumber(scalaVersion.value).matchesSemVer(SemanticSelector("<2.13.2"))) {
        Seq(
          "-Xfatal-warnings",
          "-deprecation",
          "-Ywarn-unused-import",
          "-P:silencer:globalFilters=.*deprecated.*" //regex to catch deprecation warnings and suppress them
        )
      } else {
        Seq(
          // replace -Xfatal-warnings with fine-grained configuration, since 2.13.2
          // verbose warning on deprecation, error on all others
          // see `scalac -Wconf:help` for details
          "-Wconf:cat=deprecation:wv,any:e",
          // 2.13-specific warning hits to be muted (as narrowly as possible) and addressed separately
          // TODO(SPARK-33499): Enable this option when Scala 2.12 is no longer supported.
          // "-Wunused:imports",
          "-Wconf:cat=lint-multiarg-infix:wv",
          "-Wconf:cat=other-nullary-override:wv",
          "-Wconf:cat=other-match-analysis&site=org.apache.spark.sql.catalyst.catalog.SessionCatalog.lookupFunction.catalogFunction:wv",
          "-Wconf:cat=other-pure-statement&site=org.apache.spark.streaming.util.FileBasedWriteAheadLog.readAll.readFile:wv",
          "-Wconf:cat=other-pure-statement&site=org.apache.spark.scheduler.OutputCommitCoordinatorSuite.<local OutputCommitCoordinatorSuite>.futureAction:wv",
          "-Wconf:cat=other-pure-statement&site=org.apache.spark.sql.streaming.sources.StreamingDataSourceV2Suite.testPositiveCase.\\$anonfun:wv"
        )
      }
    }
  )

  lazy val sharedSettings = sparkGenjavadocSettings ++
                            compilerWarningSettings ++
      (if (sys.env.contains("NOLINT_ON_COMPILE")) Nil else enableScalaStyle) ++ Seq(
    exportJars in Compile := true,
    exportJars in Test := false,
    javaHome := sys.env.get("JAVA_HOME")
      .orElse(sys.props.get("java.home").map { p => new File(p).getParentFile().getAbsolutePath() })
      .map(file),
    publishMavenStyle := true,
    unidocGenjavadocVersion := "0.16",

    // Override SBT's default resolvers:
    resolvers := Seq(
      // Google Mirror of Maven Central, placed first so that it's used instead of flaky Maven Central.
      // See https://storage-download.googleapis.com/maven-central/index.html for more info.
      "gcs-maven-central-mirror" at "https://maven-central.storage-download.googleapis.com/maven2/",
      DefaultMavenRepository,
      Resolver.mavenLocal,
      Resolver.file("ivyLocal", file(Path.userHome.absolutePath + "/.ivy2/local"))(Resolver.ivyStylePatterns)
    ),
    externalResolvers := resolvers.value,
    otherResolvers := SbtPomKeys.mvnLocalRepository(dotM2 => Seq(Resolver.file("dotM2", dotM2))).value,
    publishLocalConfiguration in MavenCompile := PublishConfiguration()
        .withResolverName("dotM2")
        .withArtifacts(packagedArtifacts.value.toVector)
        .withLogging(ivyLoggingLevel.value),
    publishLocalConfiguration in SbtCompile := PublishConfiguration()
        .withResolverName("ivyLocal")
        .withArtifacts(packagedArtifacts.value.toVector)
        .withLogging(ivyLoggingLevel.value),
    publishMavenStyle in MavenCompile := true,
    publishMavenStyle in SbtCompile := false,
    publishLocal in MavenCompile := publishTask(publishLocalConfiguration in MavenCompile).value,
    publishLocal in SbtCompile := publishTask(publishLocalConfiguration in SbtCompile).value,
    publishLocal := Seq(publishLocal in MavenCompile, publishLocal in SbtCompile).dependOn.value,

    javacOptions in (Compile, doc) ++= {
      val versionParts = System.getProperty("java.version").split("[+.\\-]+", 3)
      var major = versionParts(0).toInt
      if (major == 1) major = versionParts(1).toInt
      if (major >= 8) Seq("-Xdoclint:all", "-Xdoclint:-missing") else Seq.empty
    },

    javaVersion := SbtPomKeys.effectivePom.value.getProperties.get("java.version").asInstanceOf[String],

    javacOptions in Compile ++= Seq(
      "-encoding", UTF_8.name(),
      "-source", javaVersion.value
    ),
    // This -target and Xlint:unchecked options cannot be set in the Compile configuration scope since
    // `javadoc` doesn't play nicely with them; see https://github.com/sbt/sbt/issues/355#issuecomment-3817629
    // for additional discussion and explanation.
    javacOptions in (Compile, compile) ++= Seq(
      "-target", javaVersion.value,
      "-Xlint:unchecked"
    ),

    scalacOptions in Compile ++= Seq(
      s"-target:jvm-${javaVersion.value}",
      "-sourcepath", (baseDirectory in ThisBuild).value.getAbsolutePath  // Required for relative source links in scaladoc
    ),

    SbtPomKeys.profiles := profiles,

    // Remove certain packages from Scaladoc
    scalacOptions in (Compile, doc) := Seq(
      "-groups",
      "-skip-packages", Seq().mkString(":"),
      "-doc-title", "SANSA-Stack " + version.value.replaceAll("-SNAPSHOT", "") + " ScalaDoc"
    ) ++ {
      // Do not attempt to scaladoc javadoc comments under 2.12 since it can't handle inner classes
      if (scalaBinaryVersion.value == "2.12") Seq("-no-java-comments") else Seq.empty
    },

    // disable Mima check for all modules,
    // to be enabled in specific ones that have previous artifacts
    MimaKeys.mimaFailOnNoPrevious := false,

    // To prevent intermittent compilation failures, see also SPARK-33297
    // Apparently we can remove this when we use JDK 11.
    Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
  )

  def enable(settings: Seq[Setting[_]])(projectRef: ProjectRef) = {
    val existingSettings = projectsMap.getOrElse(projectRef.project, Seq[Setting[_]]())
    projectsMap += (projectRef.project -> (existingSettings ++ settings))
  }

  // Note ordering of these settings matter.
  /* Enable shared settings on all projects */
  (allProjects ++ Seq(sansa, tools))
    .foreach(enable(sharedSettings ++ DependencyOverrides.settings ++
      ExcludedDependencies.settings ++ Checkstyle.settings))

  /* Enable tests settings for all projects except examples, assembly and tools */
  (allProjects).foreach(enable(TestSettings.settings))


  /* Generate and pick the spark build info from extra-resources */
//  enable(Core.settings)(core)


  /* Enable unidoc only for the root spark project */
  enable(Unidoc.settings)(sansa)


  // SPARK-14738 - Remove docker tests from main Spark build
  // enable(DockerIntegrationTests.settings)(dockerIntegrationTests)

  // TODO: move this to its upstream project.
  override def projectDefinitions(baseDirectory: File): Seq[Project] = {
    super.projectDefinitions(baseDirectory).map { x =>
      if (projectsMap.exists(_._1 == x.id)) x.settings(projectsMap(x.id): _*)
      else x.settings(Seq[Setting[_]](): _*)
    }
  }

}

object Unsafe {
  lazy val settings = Seq(
    // This option is needed to suppress warnings from sun.misc.Unsafe usage
    javacOptions in Compile += "-XDignore.symbol.file"
  )
}



/**
 * Overrides to work around sbt's dependency resolution being different from Maven's.
 */
object DependencyOverrides {
  lazy val guavaVersion = sys.props.get("guava.version").getOrElse("14.0.1")
  lazy val settings = Seq(
    dependencyOverrides += "com.google.guava" % "guava" % guavaVersion,
    dependencyOverrides += "xerces" % "xercesImpl" % "2.12.0",
    dependencyOverrides += "jline" % "jline" % "2.14.6",
    dependencyOverrides += "org.apache.avro" % "avro" % "1.8.2")
}

/**
 * This excludes library dependencies in sbt, which are specified in maven but are
 * not needed by sbt build.
 */
object ExcludedDependencies {
  lazy val settings = Seq(
    libraryDependencies ~= { libs => libs.filterNot(_.name == "groovy-all") }
  )
}


object Unidoc {

  import BuildCommons._
  import sbtunidoc.BaseUnidocPlugin
  import sbtunidoc.JavaUnidocPlugin
  import sbtunidoc.ScalaUnidocPlugin
  import sbtunidoc.BaseUnidocPlugin.autoImport._
  import sbtunidoc.GenJavadocPlugin.autoImport._
  import sbtunidoc.JavaUnidocPlugin.autoImport._
  import sbtunidoc.ScalaUnidocPlugin.autoImport._

  private def ignoreUndocumentedPackages(packages: Seq[Seq[File]]): Seq[Seq[File]] = {
    packages
      .map(_.filterNot(_.getName.contains("$")))
  }

  private def ignoreClasspaths(classpaths: Seq[Classpath]): Seq[Classpath] = {
    classpaths
  }

  val unidocSourceBase = settingKey[String]("Base URL of source links in Scaladoc.")

  lazy val settings = BaseUnidocPlugin.projectSettings ++
                      ScalaUnidocPlugin.projectSettings ++
                      JavaUnidocPlugin.projectSettings ++
                      Seq (
    publish := {},

    unidocProjectFilter in(ScalaUnidoc, unidoc) :=
      inAnyProject,
    unidocProjectFilter in(JavaUnidoc, unidoc) :=
      inAnyProject,

    unidocAllClasspaths in (ScalaUnidoc, unidoc) := {
      ignoreClasspaths((unidocAllClasspaths in (ScalaUnidoc, unidoc)).value)
    },

    unidocAllClasspaths in (JavaUnidoc, unidoc) := {
      ignoreClasspaths((unidocAllClasspaths in (JavaUnidoc, unidoc)).value)
    },

    // Skip actual catalyst, but include the subproject.
    // Catalyst is not public API and contains quasiquotes which break scaladoc.
    unidocAllSources in (ScalaUnidoc, unidoc) := {
      ignoreUndocumentedPackages((unidocAllSources in (ScalaUnidoc, unidoc)).value)
    },

    // Skip class names containing $ and some internal packages in Javadocs
    unidocAllSources in (JavaUnidoc, unidoc) := {
      ignoreUndocumentedPackages((unidocAllSources in (JavaUnidoc, unidoc)).value)
        .map(_.filterNot(_.getCanonicalPath.contains("org/apache/hadoop")))
    },

    javacOptions in (JavaUnidoc, unidoc) := Seq(
      "-windowtitle", "SANSA Stack " + version.value.replaceAll("-SNAPSHOT", "") + " JavaDoc",
      "-public",
      "-noqualifier", "java.lang",
      "-tag", """example:a:Example\:""",
      "-tag", """note:a:Note\:""",
      "-tag", "group:X",
      "-tag", "tparam:X",
      "-tag", "constructor:X",
      "-tag", "todo:X",
      "-tag", "groupname:X"
    ),

    // Use GitHub repository for Scaladoc source links
    unidocSourceBase := s"https://github.com/SANSA-Stack/SANSA-Stack/tree/v${version.value}",

    scalacOptions in (ScalaUnidoc, unidoc) ++= Seq(
      "-groups", // Group similar methods together based on the @group annotation.
      "-skip-packages", "org.apache.hadoop",
      "-sourcepath", (baseDirectory in ThisBuild).value.getAbsolutePath
    ) ++ (
      // Add links to sources when generating Scaladoc for a non-snapshot release
      if (!isSnapshot.value) {
        Opts.doc.sourceUrl(unidocSourceBase.value + "€{FILE_PATH}.scala")
      } else {
        Seq()
      }
    )
  )
}

object Checkstyle {
  lazy val settings = Seq(
    checkstyleSeverityLevel := Some(CheckstyleSeverityLevel.Error),
    javaSource in (Compile, checkstyle) := baseDirectory.value / "src/main/java",
    javaSource in (Test, checkstyle) := baseDirectory.value / "src/test/java",
    checkstyleConfigLocation := CheckstyleConfigLocation.File("dev/checkstyle.xml"),
    checkstyleOutputFile := baseDirectory.value / "target/checkstyle-output.xml",
    checkstyleOutputFile in Test := baseDirectory.value / "target/checkstyle-output.xml"
  )
}

object TestSettings {
  import BuildCommons._
  private val defaultExcludedTags = Seq("org.apache.spark.tags.ChromeUITest")

  lazy val settings = Seq (
    // Fork new JVMs for tests and set Java options for those
    fork := true,
    // Setting SPARK_DIST_CLASSPATH is a simple way to make sure any child processes
    // launched by the tests have access to the correct test-time classpath.
    envVars in Test ++= Map(
      "SPARK_DIST_CLASSPATH" ->
        (fullClasspath in Test).value.files.map(_.getAbsolutePath)
          .mkString(File.pathSeparator).stripSuffix(File.pathSeparator),
      "SPARK_PREPEND_CLASSES" -> "1",
      "SPARK_SCALA_VERSION" -> scalaBinaryVersion.value,
      "SPARK_TESTING" -> "1",
      "JAVA_HOME" -> sys.env.get("JAVA_HOME").getOrElse(sys.props("java.home"))),
    javaOptions in Test += s"-Djava.io.tmpdir=$testTempDir",
    javaOptions in Test += "-Dsun.io.serialization.extendedDebugInfo=false",
    javaOptions in Test += "-Dderby.system.durability=test",
    javaOptions in Test += "-Dio.netty.tryReflectionSetAccessible=true",
    javaOptions in Test ++= System.getProperties.asScala.filter(_._1.startsWith("spark"))
      .map { case (k,v) => s"-D$k=$v" }.toSeq,
    javaOptions in Test += "-ea",
    // SPARK-29282 This is for consistency between JDK8 and JDK11.
    javaOptions in Test ++= "-Xmx4g -Xss4m -XX:+UseParallelGC -XX:-UseDynamicNumberOfGCThreads"
      .split(" ").toSeq,
    javaOptions += "-Xmx3g",
    // Exclude tags defined in a system property
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest,
      sys.props.get("test.exclude.tags").map { tags =>
        tags.split(",").flatMap { tag => Seq("-l", tag) }.toSeq
      }.getOrElse(Nil): _*),
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest,
      sys.props.get("test.default.exclude.tags").map(tags => tags.split(",").toSeq)
        .map(tags => tags.filter(!_.trim.isEmpty)).getOrElse(defaultExcludedTags)
        .flatMap(tag => Seq("-l", tag)): _*),
    testOptions in Test += Tests.Argument(TestFrameworks.JUnit,
      sys.props.get("test.exclude.tags").map { tags =>
        Seq("--exclude-categories=" + tags)
      }.getOrElse(Nil): _*),
    // Include tags defined in a system property
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest,
      sys.props.get("test.include.tags").map { tags =>
        tags.split(",").flatMap { tag => Seq("-n", tag) }.toSeq
      }.getOrElse(Nil): _*),
    testOptions in Test += Tests.Argument(TestFrameworks.JUnit,
      sys.props.get("test.include.tags").map { tags =>
        Seq("--include-categories=" + tags)
      }.getOrElse(Nil): _*),
    // Show full stack trace and duration in test cases.
    testOptions in Test += Tests.Argument("-oDF"),
    testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),
    // Enable Junit testing.
    libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test",
    // `parallelExecutionInTest` controls whether test suites belonging to the same SBT project
    // can run in parallel with one another. It does NOT control whether tests execute in parallel
    // within the same JVM (which is controlled by `testForkedParallel`) or whether test cases
    // within the same suite can run in parallel (which is a ScalaTest runner option which is passed
    // to the underlying runner but is not a SBT-level configuration). This needs to be `true` in
    // order for the extra parallelism enabled by `SparkParallelTestGrouping` to take effect.
    // The `SERIAL_SBT_TESTS` check is here so the extra parallelism can be feature-flagged.
    parallelExecution in Test := { if (sys.env.contains("SERIAL_SBT_TESTS")) false else true },
    // Make sure the test temp directory exists.
    resourceGenerators in Test += Def.macroValueI(resourceManaged in Test map { outDir: File =>
      var dir = new File(testTempDir)
      if (!dir.isDirectory()) {
        // Because File.mkdirs() can fail if multiple callers are trying to create the same
        // parent directory, this code tries to create parents one at a time, and avoids
        // failures when the directories have been created by somebody else.
        val stack = new Stack[File]()
        while (!dir.isDirectory()) {
          stack.push(dir)
          dir = dir.getParentFile()
        }

        while (stack.nonEmpty) {
          val d = stack.pop()
          require(d.mkdir() || d.isDirectory(), s"Failed to create directory $d")
        }
      }
      Seq.empty[File]
    }).value,
    concurrentRestrictions in Global := {
      // The number of concurrent test groups is empirically chosen based on experience
      // with Jenkins flakiness.
      if (sys.env.contains("SERIAL_SBT_TESTS")) (concurrentRestrictions in Global).value
      else Seq(Tags.limit(Tags.ForkedTestGroup, 4))
    }
  )

}
