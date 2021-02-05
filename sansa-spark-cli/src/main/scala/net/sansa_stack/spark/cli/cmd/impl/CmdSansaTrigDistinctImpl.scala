package net.sansa_stack.spark.cli.cmd.impl

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import net.sansa_stack.query.spark.api.domain.ResultSetSpark
import net.sansa_stack.query.spark.ops.rdd.RddOfBindingOps
import net.sansa_stack.rdf.spark.model.rdd.RddOfDatasetOps
import net.sansa_stack.spark.cli.cmd.{CmdSansaTrigDistinct, CmdSansaTrigQuery}
import org.aksw.jena_sparql_api.rx.RDFLanguagesEx
import org.aksw.jena_sparql_api.utils.io.WriterStreamRDFBaseWrapper
import org.apache.jena.query.{Dataset, QueryFactory, Syntax}
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.system.{StreamRDFOps, StreamRDFWriter}
import org.apache.jena.riot.writer.WriterStreamRDFBase
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat, ResultSetMgr}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory


/**
 * Called from the Java class [[CmdSansaTrigQuery]]
 */
class CmdSansaTrigDistinctImpl
object CmdSansaTrigDistinctImpl {
  private val logger = LoggerFactory.getLogger(getClass)
  // JenaSystem.init()

  def run(cmd: CmdSansaTrigDistinct): Integer = {

    val stopwatch = Stopwatch.createStarted()

    // val resultSetFormats = RDFLanguagesEx.getResultSetFormats
    // val outLang = RDFLanguagesEx.findLang(cmd.outFormat, resultSetFormats)

//    if (outLang == null) {
//      throw new IllegalArgumentException("No result set format found for " + cmd.outFormat)
//    }

//    logger.info("Detected registered result set format: " + outLang)

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
      .config("spark.kryoserializer.buffer.max", "1000") // MB
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

    val effectiveRdd = RddOfDatasetOps.groupNamedGraphsByGraphIri(initialRdd)


    // Just for testing
    // val prefixes = RDFDataMgr.loadModel("rdf-prefixes/prefix.cc.2019-12-17.ttl")
    val prefixes = ModelFactory.createDefaultModel

    val rdfFormat = RDFFormat.TRIG_BLOCKS
    val out = Files.newOutputStream(Paths.get("output.trig"), StandardOpenOption.WRITE, StandardOpenOption.CREATE)
    // System.out
    val writer = StreamRDFWriter.getWriterStream(out, rdfFormat, null)
    writer.start
    StreamRDFOps.sendPrefixesToStream(prefixes, writer)

    // val it = effectiveRdd.collect
    val it = effectiveRdd.toLocalIterator
    for (dataset <- it) {
      StreamRDFOps.sendDatasetToStream(dataset.asDatasetGraph, writer)
    }
    writer.finish


    // effectiveRdd.saveAsFile("outfile.trig.bz2", prefixes, RDFFormat.TRIG_BLOCKS)

    // effectiveRdd.coalesce(1)


    // ResultSetMgr.write(System.out, resultSetSpark.collectToTable().toResultSet, outLang)

    logger.info("Processing time: " + stopwatch.elapsed(TimeUnit.SECONDS) + " seconds")

    0 // exit code
  }
}

