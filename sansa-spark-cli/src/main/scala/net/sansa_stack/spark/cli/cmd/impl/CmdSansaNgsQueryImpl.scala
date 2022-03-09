package net.sansa_stack.spark.cli.cmd.impl

import net.sansa_stack.query.spark.api.domain.ResultSetSpark
import org.aksw.jena_sparql_api.rx.RDFLanguagesEx
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.commons.lang3.time.StopWatch
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.jena.query.{Dataset, QueryFactory, ResultSet, Syntax}
import org.apache.jena.riot.{Lang, ResultSetMgr}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.net.URI
import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import net.sansa_stack.query.spark.rdd.op.RddOfBindingsOps
import net.sansa_stack.rdf.spark.rdd.op.RddOfDatasetsOps
import net.sansa_stack.spark.cli.cmd.CmdSansaNgsQuery
import org.aksw.jenax.arq.dataset.api.DatasetOneNg

/**
 * Called from the Java class [[CmdSansaNgsQuery]]
 */
class CmdSansaNgsQueryImpl
object CmdSansaNgsQueryImpl {
  private val logger = LoggerFactory.getLogger(getClass)
    // JenaSystem.init()


  def run(cmd: CmdSansaNgsQuery): Integer = {

    val resultSetFormats = RDFLanguagesEx.getResultSetFormats
    val outLang = RDFLanguagesEx.findLang(cmd.outFormat, resultSetFormats)

    if (outLang == null) {
      throw new IllegalArgumentException("No result set format found for " + cmd.outFormat)
    }

    logger.info("Detected registered result set format: " + outLang)

    val queryString = scala.reflect.io.File(cmd.queryFile).slurp()

    logger.info("Loaded query " + queryString)
    val query = QueryFactory.create(queryString, Syntax.syntaxARQ)

    import collection.JavaConverters._

    val trigFiles = cmd.trigFiles.asScala
      .map(pathStr => Paths.get(pathStr).toAbsolutePath)
      .toList

    val spark = SparkSession.builder
      .appName(s"Trig Query ( $trigFiles )")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", String.join(
        ", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator"))
      .config("spark.sql.crossJoin.enabled", true)
      .getOrCreate()

    val hadoopConf = spark.sparkContext.hadoopConfiguration

    val paths = cmd.trigFiles.asScala
      .flatMap(pathStr => {
        var r: Iterator[(FileSystem, Path)] = Iterator()
        try {
          val uri = new URI(pathStr)
          // TODO Use try-with-resources for the filesystem?
          val fs = FileSystem.get(uri, hadoopConf)
          val path = new Path(pathStr)
          fs.resolvePath(path)
          r = Iterator((fs, path))
        } catch {
          case e: Throwable => logger.error(ExceptionUtils.getRootCauseMessage(e))
        }
        r
      })
      .filter { case (fs, file) => fs.isFile(file) }
      .map(_._2)
      .toList

    /*
    val validPaths = paths
      .filter(_.getFileSystem(hadoopConf).get)
      .filter(!fileSystem.isFile(_))
      .toSet
*/
    val validPathSet = paths.toSet

    val invalidPaths = paths.toSet.diff(validPathSet)
    if (!invalidPaths.isEmpty) {
      throw new IllegalArgumentException("The following paths are invalid (do not exist or are not a (readable) file): " + invalidPaths)
    }

    import net.sansa_stack.rdf.spark.io._

    val initialRdd: RDD[DatasetOneNg] = spark.sparkContext.union(
      validPathSet
        .map(path => spark.datasets(Lang.TRIG)(path.toString)).toSeq)

    val effectiveRdd = if (cmd.makeDistinct) RddOfDatasetsOps.groupNamedGraphsByGraphIri(initialRdd)
      else initialRdd

    val stopwatch = StopWatch.createStarted()

    val resultSetSpark: ResultSetSpark =
      RddOfBindingsOps.execSparqlSelect(effectiveRdd, query)

    ResultSetMgr.write(System.out, ResultSet.adapt(resultSetSpark.collectToTable().toRowSet), outLang)

    logger.info("Processing time: " + stopwatch.getTime(TimeUnit.SECONDS) + " seconds")

    0 // exit code
  }
}
