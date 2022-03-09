package net.sansa_stack.spark.cli.cmd.impl

import net.sansa_stack.query.spark.api.domain.ResultSetSpark
import net.sansa_stack.spark.cli.cmd.{CmdSansaTarql, CmdSansaTrigQuery}
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
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import net.sansa_stack.query.spark.rdd.op.RddOfBindingsOps
import net.sansa_stack.rdf.spark.rdd.op.RddOfDatasetsOps
import net.sansa_stack.spark.io.csv.input.CsvDataSources
import net.sansa_stack.spark.io.rdf.output.{RddRdfWriterFactory, RddRdfWriterSettings}
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOfBindingsOps
import org.aksw.jena_sparql_api.common.DefaultPrefixes
import org.aksw.jenax.arq.dataset.api.DatasetOneNg
import org.aksw.jenax.stmt.core.SparqlStmtMgr
import org.apache.commons.csv.CSVFormat
import org.apache.jena.shared.impl.PrefixMappingImpl
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.api.java.JavaSparkContext

/**
 * Called from the Java class [[CmdSansaTrigQuery]]
 */
class CmdSansaTarqlImpl
object CmdSansaTarqlImpl {
  private val logger = LoggerFactory.getLogger(getClass)
  // JenaSystem.init()


  def run(cmd: CmdSansaTarql): Integer = {

    val query = SparqlStmtMgr.loadQuery(cmd.queryFile, DefaultPrefixes.get)
    logger.info("Loaded query " + query)

    if (!query.isConstructType && !query.isConstructQuad) {
      throw new IllegalArgumentException("Query must be of CONSTRUCT type (triples or quads)")
    }

    val rddRdfWriterFactory = RddRdfWriterFactory.create()
      .setGlobalPrefixMapping(new PrefixMappingImpl())
      .setOutputFormat(cmd.outFormat)
      .setMapQuadsToTriplesForTripleLangs(true)
      // .setAllowOverwriteFiles(true)
      .setPartitionFolder(cmd.outFolder)
      .setTargetFile(cmd.outFile)
      // .setUseElephas(true)
      .setDeletePartitionFolderAfterMerge(true)
      .validate()

    // val queryString = scala.reflect.io.File(cmd.queryFile).slurp()

    import collection.JavaConverters._

    val trigFiles = cmd.trigFiles.asScala
      .map(pathStr => Paths.get(pathStr).toAbsolutePath)
      .toList

    var sparkSessionBuilder = SparkSession.builder
      .appName(s"Sansa Tarql ( $trigFiles )")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "1000") // MB
      .config("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator"))
      .config("spark.hadoop.mapreduce.input.fileinputformat.split.maxsize", "10000000")

    if (System.getProperty("spark.master") == null) {
      logger.info("'spark.master' not set - assuming default local[*]")
      sparkSessionBuilder = sparkSessionBuilder.master("local[*]")
    }

    val spark = sparkSessionBuilder.getOrCreate()

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

    val validPathSet = paths.toSet

    val invalidPaths = paths.toSet.diff(validPathSet)
    if (!invalidPaths.isEmpty) {
      throw new IllegalArgumentException("The following paths are invalid (do not exist or are not a (readable) file): " + invalidPaths)
    }

    val javaSparkContext = new JavaSparkContext(spark.sparkContext)

    val initialRdd: RDD[Binding] = spark.sparkContext.union(
      validPathSet
        .map(path => CsvDataSources.createRddOfBindings(javaSparkContext, path.toString,
          CSVFormat.EXCEL.builder().setSkipHeaderRecord(true).build()).rdd).toSeq)

    val stopwatch = StopWatch.createStarted()


    if (query.isConstructQuad) {
      rddRdfWriterFactory.forQuad(JavaRddOfBindingsOps.tarqlQuads(initialRdd, query)).run()
    } else if (query.isConstructType) {
      rddRdfWriterFactory.forTriple(JavaRddOfBindingsOps.tarqlTriples(initialRdd, query)).run()
    }


    logger.info("Processing time: " + stopwatch.getTime(TimeUnit.SECONDS) + " seconds")

    0 // exit code
  }
}
