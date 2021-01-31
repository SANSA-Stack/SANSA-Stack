package net.sansa_stack.spark.cli.cmd.impl

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import net.sansa_stack.query.spark.api.domain.ResultSetSpark
import net.sansa_stack.query.spark.ops.rdd.RddOfBindingOps
import net.sansa_stack.spark.cli.cmd.CmdSansaTrigQuery
import org.aksw.jena_sparql_api.rx.RDFLanguagesEx
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

    val trigFile = Paths.get(cmd.trigFile).toAbsolutePath


    val spark = SparkSession.builder
      .master(cmd.sparkMaster)
      .appName(s"SPARQL example ( $trigFile )")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", String.join(
        ", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator"))
      .config("spark.sql.crossJoin.enabled", true)
      .getOrCreate()

    import net.sansa_stack.rdf.spark.io._

    val rdd: RDD[Dataset] = spark.datasets(Lang.TRIG)(trigFile.toString)

    val stopwatch = Stopwatch.createStarted()

    val resultSetSpark: ResultSetSpark =
      RddOfBindingOps.selectWithSparql(rdd, query)

    ResultSetMgr.write(System.out, resultSetSpark.collectToTable().toResultSet, outLang)

    logger.info("Processing time: " + stopwatch.elapsed(TimeUnit.SECONDS) + " seconds")

    0 // exit code
  }
}
