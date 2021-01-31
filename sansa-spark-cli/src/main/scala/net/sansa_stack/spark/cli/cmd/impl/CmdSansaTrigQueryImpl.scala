package net.sansa_stack.spark.cli.cmd.impl

import java.nio.file.{Files, Path, Paths}
import java.util
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors

import com.google.common.base.Stopwatch
import net.sansa_stack.query.spark.api.domain.ResultSetSpark
import net.sansa_stack.query.spark.ops.rdd.RddOfBindingOps
import net.sansa_stack.rdf.spark.model.rdd.RddOfDatasetOps
import net.sansa_stack.spark.cli.cmd.CmdSansaTrigQuery
import org.aksw.jena_sparql_api.rx.RDFLanguagesEx
import org.apache.jena.ext.com.google.common.collect.Sets
import org.apache.jena.query.{Dataset, QueryFactory, Syntax}
import org.apache.jena.riot.{Lang, ResultSetMgr}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Called from the Java class [[CmdSansaTrigQuery]]
 */
class CmdSansaTrigQueryImpl
object CmdSansaTrigQueryImpl {
  private val logger = LoggerFactory.getLogger(getClass)
    // JenaSystem.init()


  def run(cmd: CmdSansaTrigQuery): Integer = {

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

    val validPaths = trigFiles
      .filter(Files.exists(_))
      .filter(!Files.isDirectory(_))
      .filter(Files.isReadable(_))
      .toSet

    val invalidPaths = trigFiles.toSet.diff(validPaths)
    if (!invalidPaths.isEmpty) {
      throw new IllegalArgumentException("The following paths are invalid (do not exist or are not a (readable) file): " + invalidPaths)
    }

    val spark = SparkSession.builder
      .master(cmd.sparkMaster)
      .appName(s"SPARQL example ( $trigFiles )")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", String.join(
        ", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator"))
      .config("spark.sql.crossJoin.enabled", true)
      .getOrCreate()

    import net.sansa_stack.rdf.spark.io._

    val initialRdd: RDD[Dataset] = validPaths
      .map(path => spark.datasets(Lang.TRIG)(path.toString))
      .reduce((a, b) => a.union(b))

    val effectiveRdd = if (cmd.makeDistinct) RddOfDatasetOps.groupNamedGraphsByGraphIri(initialRdd)
      else initialRdd


    val stopwatch = Stopwatch.createStarted()

    val resultSetSpark: ResultSetSpark =
      RddOfBindingOps.selectWithSparql(effectiveRdd, query)

    ResultSetMgr.write(System.out, resultSetSpark.collectToTable().toResultSet, outLang)

    logger.info("Processing time: " + stopwatch.elapsed(TimeUnit.SECONDS) + " seconds")

    0 // exit code
  }
}
