package net.sansa_stack.rdf.spark

import java.io.ByteArrayOutputStream
import java.util.Collections

import com.typesafe.config.{Config, ConfigFactory}

import net.sansa_stack.rdf.common.io.hadoop.TrigFileInputFormat
import net.sansa_stack.rdf.spark.io.nquads.NQuadReader
import net.sansa_stack.rdf.spark.io.stream.RiotFileInputFormat
import net.sansa_stack.rdf.spark.utils.Logging
import org.aksw.jena_sparql_api.rx.RDFLanguagesEx
import org.aksw.jena_sparql_api.utils.io.WriterStreamRDFBaseWrapper
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.hadoop.rdf.io.input.TriplesInputFormat
import org.apache.jena.hadoop.rdf.io.input.turtle.TurtleInputFormat
import org.apache.jena.hadoop.rdf.io.output.QuadsOutputFormat
import org.apache.jena.hadoop.rdf.types.{QuadWritable, TripleWritable}
import org.apache.jena.query.{Dataset => JenaDataset}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.system.{StreamRDFOps, StreamRDFWriter}
import org.apache.jena.riot.writer.WriterStreamRDFBase
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat, RDFLanguages}
import org.apache.jena.shared.PrefixMapping
import org.apache.jena.sparql.core.Quad
import org.apache.jena.sparql.util.{FmtUtils, NodeFactoryExtra}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, _}

/**
 * Wrap up implicit classes/methods to read/write RDF data from N-Triples or Turtle files into either [[DataFrame]] or
 * [[RDD]].
 */
package object io {

  /**
   * SaveMode is used to specify the expected behavior of saving an RDF dataset to a path.
   */
  object SaveMode extends Enumeration {
    type SaveMode = Value
    val

    /**
     * Overwrite mode means that when saving an RDF dataset to a path,
     * if path already exists,
     * the existing data is expected to be overwritten by the contents of the RDF dataset.
     */
    Overwrite,

    /**
     * ErrorIfExists mode means that when saving an RDF dataset to a path, if path already exists,
     * an exception is expected to be thrown.
     */
    ErrorIfExists,

    /**
     * Ignore mode means that when saving an RDF dataset to a path, if path already exists,
     * the save operation is expected to not save the contents of the RDF dataset and to not
     * change the existing data.
     */
    Ignore = Value
  }

  /**
   * Converts a Jena [[Triple]] to a Spark SQL [[Row]] with three columns.
   * @param triple the triple
   * @return the row
   */
  def toRow(triple: org.apache.jena.graph.Triple): Row = {
    toRow(Seq(triple.getSubject, triple.getPredicate, triple.getObject))
  }

  private val pm = PrefixMapping.Factory.create

  /**
   * Converts a list of Jena [[Node]] objects to a Spark SQL [[Row]].
   * The output is always a row of string values with each RDF node type being serialized as follows:
   *
   *  - URI: `http://foo.bar`
   *  - bnode: `_:123`
   *  - Literal: `"123"^^<http://some.datatype.uri>`
   *
   * @param nodes the nodes
   * @return the row
   */
  def toRow(nodes: Seq[Node]): Row = {
    // we use the Jena rendering, for URIs we omit the wrapping angle brackets, i.e. we return
    // http://foo.bar instead of <http://foo.bar> but for datatype URIs in literals we keep it
    // TODO this is basically done because of querying, but for simplicity it would be easier to return <http://foo.bar>
    Row.fromSeq(nodes.map(n => if (n.isURI) n.toString else FmtUtils.stringForNode(n, pm)))
  }

  /**
   * Converts a Spark SQL [[Row]] to a Triple.
   *
   * @note It assumes a row containing exactly 3 columns and the
   * order being `s,p,o`.
   * Moreover, we assume the following serialization of the RDF entities in the columns which also matches how we read
   * N-Triples into a DataFrame:
   *
   *  - URI: `http://foo.bar`
   *  - bnode: `_:123`
   *  - Literal: `"123"^^<http://some.datatype.uri>`
   *
   *
   * @param row the row with columns `s,p,o`
   * @return the parsed triple
   */
  def fromRow(row: Row): org.apache.jena.graph.Triple = {
    val sStr = row.getString(0)
    val s = if (sStr.startsWith("_:")) NodeFactory.createBlankNode(sStr)
            else NodeFactoryExtra.parseNode(s"<$sStr>")

    // as common in RDF, we assume URIs only for predicate
    val p = NodeFactoryExtra.parseNode("<" + row.getString(1) + ">")

    val oStr = row.getString(2)
    val o = if (oStr.startsWith("_:")) { // bnode
      NodeFactory.createBlankNode(oStr)
    } else if (oStr.startsWith("http") && !oStr.contains("^^")) { // URI
      NodeFactory.createURI(oStr)
    } else { // literal
      var lit = oStr
//      val idx = oStr.indexOf("^^")
//      if (idx > 0) {
//        val first = oStr.substring(0, idx)
//        val second = "<" + oStr.substring(idx + 2).trim + ">"
//        lit = first + "^^" + second
//      }
      NodeFactoryExtra.parseNode(lit)
    }

    Triple.create(s, p, o)
  }

  // the DataFrame methods

  /**
   * Adds methods, `ntriples` and `turtle`, to [[DataFrameWriter]] that allows to write N-Triples files.
   */
  implicit class RDFDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def rdf: String => Unit = writer.format("ntriples").save
    def ntriples: String => Unit = writer.format("ntriples").save
  }

  /**
   * Adds methods, `rdf`, `ntriples` and `turtle`, to [[DataFrameReader]] that allows to read N-Triples and Turtle
   * files.
   */
  implicit class RDFDataFrameReader(reader: DataFrameReader) extends Logging {
    @transient lazy val conf: Config = ConfigFactory.load("rdf_loader")
    /**
     * Load RDF data into a `DataFrame`.
     * @param lang the RDF language
     * @return a [[DataFrame]][(String, String, String)]
     */
    def rdf(lang: Lang): String => DataFrame = lang match {
      case i if lang == Lang.NTRIPLES => ntriples
      case _ => reader.format("rdf").option("lang", lang.getLabel).load
    }

    /**
     * Load RDF data in N-Triples syntax into a [[DataFrame]] with columns `s`, `p`, and `o`.
     * @return a [[DataFrame]][(String, String, String)]
     */
    def ntriples: String => DataFrame = {
      logDebug(s"Parsing N-Triples with ${conf.getString("rdf.ntriples.parser")} ...")
      reader.format("ntriples").load
    }
    /**
     * Load RDF data in Turtle syntax into a [[DataFrame]] with columns `s`, `p`, and `o`.
     * @return a [[DataFrame]][(String, String, String)]
     */
    def turtle: String => DataFrame = reader.format("rdf").option("lang", Lang.TURTLE.getLabel).load

    /**
     * Load RDF data in RDF/XML syntax into a [[DataFrame]] with columns `s`, `p`, and `o`.
     * @return a [[DataFrame]][(String, String, String)]
     */
    def rdfxml: String => DataFrame = reader.format("rdf").option("lang", Lang.RDFXML.getLabel).load
  }

  // the RDD methods

  /**
   * Adds methods, `saveAsNTriplesFile` to an RDD[Triple] that allows to write N-Triples files.
   */
  implicit class RDFWriter[T](triples: RDD[Triple]) {


    //     * @param singleFile write to a single file only (internally, this is done by RDD::coalesce(1) function)
    //     *                   and is usually not recommended for large dataset because all data has to be moved
    //     *                   to a single node).

    /**
     * Save the data in N-Triples format.
     *
     * @param path the path where the N-Triples file(s) will be written to
     * @param mode the expected behavior of saving the data to a data source
     * @param exitOnError whether to stop if an error occurred
     */
    def saveAsNTriplesFile(path: String,
                           mode: io.SaveMode.Value = SaveMode.ErrorIfExists,
                           exitOnError: Boolean = false): Unit = {

      val fsPath = new Path(path)
      val fs = fsPath.getFileSystem(triples.sparkContext.hadoopConfiguration)

      val doSave = if (fs.exists(fsPath)) {
        mode match {
          case SaveMode.Overwrite =>
            fs.delete(fsPath, true)
            true
          case SaveMode.ErrorIfExists =>
            sys.error(s"Given path $path already exists!")
            if (exitOnError) sys.exit(1)
            false
          case SaveMode.Ignore => false
          case _ =>
            throw new IllegalStateException(s"Unsupported save mode $mode ")
        }
      } else {
        true
      }

      import scala.collection.JavaConverters._
      // save only if there was no failure with the path before
      if (doSave) triples
        .mapPartitions(p => { // process each partition
          // check if partition is empty
          if (p.hasNext) {
            val os = new ByteArrayOutputStream()
            RDFDataMgr.writeTriples(os, p.asJava)
            Collections.singleton(os.toString("UTF-8").trim).iterator().asScala
          } else {
            Iterator()
          }
        })
        .saveAsTextFile(path)
    }
  }



  /**
   * Adds method `saveAsNQuadsFile` to an RDD[Quad] that allows to write N-Quads files.
   */
  implicit class RDFQuadsWriter[T](quads: RDD[Quad]) {

    /**
     * Save the data in N-Quads format.
     *
     * @param path the path where the N-Quads file(s) will be written to
     * @param mode the expected behavior of saving the data to a data source
     * @param exitOnError whether to stop if an error occurred
     */
    def saveAsNQuadsFile(path: String,
             mode: io.SaveMode.Value = SaveMode.ErrorIfExists,
             exitOnError: Boolean = false): Unit = {

      val fsPath = new Path(path)
      val fs = fsPath.getFileSystem(quads.sparkContext.hadoopConfiguration)

      val doSave = if (fs.exists(fsPath)) {
        mode match {
          case SaveMode.Overwrite =>
            fs.delete(fsPath, true)
            true
          case SaveMode.ErrorIfExists =>
            sys.error(s"Given path $path already exists!")
            if (exitOnError) sys.exit(1)
            false
          case SaveMode.Ignore => false
          case _ =>
            throw new IllegalStateException(s"Unsupported save mode $mode ")
        }
      } else {
        true
      }

      import scala.collection.JavaConverters._
      // save only if there was no failure with the path before
      if (doSave) quads
        .mapPartitions(p => { // process each partition
          // check if partition is empty
          if (p.hasNext) {
            val os = new ByteArrayOutputStream()
            RDFDataMgr.writeQuads(os, p.asJava)
            Collections.singleton(os.toString("UTF-8").trim).iterator().asScala
          } else {
            Iterator()
          }
        })
        .saveAsTextFile(path)
    }

    def save(path: String): Unit = {
      // determine language based on file extension
      val lang = RDFLanguages.filenameToLang(path)

      // unknown format
      if (!RDFLanguages.isQuads(lang)) {
        throw new IllegalArgumentException(s"couldn't determine syntax for RDF quads based on file extension in given path $path")
      }

      // N-Triples can be handle efficiently via file splits
      if (lang == Lang.NQUADS) {
        saveAsNQuadsFile(path)
      } else { // others can't
        val sc = quads.sparkContext

        val confHadoop = sc.hadoopConfiguration

        quads.zipWithIndex().map{case (k, v) => (v, k)}
          .saveAsNewAPIHadoopFile(path, classOf[LongWritable], classOf[QuadWritable], classOf[QuadsOutputFormat[LongWritable]], confHadoop)
      }
    }
  }


  def makeString(prefixMapping: PrefixMapping, rdfFormat: RDFFormat): String = {
    val tmp: Model = ModelFactory.createDefaultModel
    tmp.setNsPrefixes(prefixMapping)
    val baos = new ByteArrayOutputStream()
    RDFDataMgr.write(baos, tmp, RDFFormat.TURTLE_PRETTY)
    baos.toString("UTF-8").trim
  }


  /**
   * Adds method `saveAsNQuadsFile` to an RDD[Quad] that allows to write N-Quads files.
   */
  implicit class RDFChunkWriter[T](quads: RDD[JenaDataset]) {

    /**
     * Save the data in N-Quads format.
     *
     * @param path the path where the N-Quads file(s) will be written to
     * @param mode the expected behavior of saving the data to a data source
     * @param exitOnError whether to stop if an error occurred
     */
    def saveAsFile(path: String,
                   prefixMapping: PrefixMapping,
                   rdfFormat: RDFFormat,
                   mode: io.SaveMode.Value = SaveMode.ErrorIfExists,
                   exitOnError: Boolean = false): Unit = {

      val fsPath = new Path(path)
      val fs = fsPath.getFileSystem(quads.sparkContext.hadoopConfiguration)

      val doSave = if (fs.exists(fsPath)) {
        mode match {
          case SaveMode.Overwrite =>
            fs.delete(fsPath, true)
            true
          case SaveMode.ErrorIfExists =>
            sys.error(s"Given path $path already exists!")
            if (exitOnError) sys.exit(1)
            false
          case SaveMode.Ignore => false
          case _ =>
            throw new IllegalStateException(s"Unsupported save mode $mode ")
        }
      } else {
        true
      }

      import scala.collection.JavaConverters._

      val prefixStr: String =
        if (prefixMapping != null && !prefixMapping.hasNoMappings) {
          makeString(prefixMapping, RDFFormat.TURTLE_PRETTY)
        } else {
          null
        }


      val prefixMappingBc = quads.sparkContext.broadcast(prefixMapping)

      // save only if there was no failure with the path before
      if (doSave) {
        val rdfFormatStr = rdfFormat.toString

        val dataBlocks = quads
          .mapPartitions(p => {
            if (p.hasNext) {
              val rdfFormat = RDFLanguagesEx.findRdfFormat(rdfFormatStr)

              val baos = new ByteArrayOutputStream()
              val rawWriter = StreamRDFWriter.getWriterStream(baos, rdfFormat, null)
              val writer = WriterStreamRDFBaseWrapper.wrapWithFixedPrefixes(
                prefixMappingBc.value, rawWriter.asInstanceOf[WriterStreamRDFBase])

              while (p.hasNext) {
                val ds: JenaDataset = p.next
                StreamRDFOps.sendDatasetToStream(ds.asDatasetGraph(), writer)
              }

              Collections.singleton(baos.toString("UTF-8").trim).iterator().asScala
            } else {
              Iterator()
            }
          })

        // If there are prefixes then serialize them and prepend
        // their string to the output

        val allBlocks: RDD[String] =
          if (prefixStr != null) {
            val prefixRdd = quads.sparkContext.parallelize(Seq(prefixStr))
            prefixRdd.union(dataBlocks)
          } else {
            dataBlocks
          }

          allBlocks
            .saveAsTextFile(path)
      }
    }
  }



  /**
   * Adds methods, `rdf(lang: Lang)`, `ntriples`, `nquads`, and `turtle`, to [[SparkSession]] that allows to read
   * N-Triples, N-Quads and Turtle files.
   */
  implicit class RDFReader(spark: SparkSession) {

    /**
     * Load RDF data into an [[RDD]][Triple].
     *
     * Syntax is determined based on the file extension:
     * If the URI ends ".rdf", it is assumed to be RDF/XML.
     * If the URI ends ".nt", it is assumed to be N-Triples.
     * If the URI ends ".ttl", it is assumed to be Turtle.
     * If the URI ends ".owl", it is assumed to be RDF/XML.
     *
     * @return the [[RDD]] of RDF triples
     */
    def rdf(path: String): RDD[Triple] = {
      // determine language based on file extension
      val lang = RDFLanguages.filenameToLang(path)

      // unknown format
      if (!RDFLanguages.isTriples(lang)) {
        throw new IllegalArgumentException(s"couldn't determine syntax for RDF triples based on file extension in given path $path")
      }

      // N-Triples can be handle efficiently via file splits
      if (lang == Lang.NTRIPLES) {
        NTripleReader.load(spark, path)
      } else { // others can't
        val confHadoop = spark.sparkContext.hadoopConfiguration

        // 1. parse the Turtle file into an RDD[String] with each entry containing a full Turtle snippet
        val rdd = spark.sparkContext.newAPIHadoopFile(
          path, classOf[TriplesInputFormat], classOf[LongWritable], classOf[TripleWritable], confHadoop)
          .map { case (_, v) => v.get() }

        rdd
      }
    }



    /**
     * Load RDF data into an [[RDD]][Triple]. Currently, N-Triples, Turtle, RDF/XML and Trix syntax are supported.
     * @param lang the RDF language (N-Triples, Turtle, RDF/XML, Trix)
     * @return the [[RDD]] of RDF triples
     */
    def rdf(lang: Lang): String => RDD[Triple] = lang match {
      case i if lang == Lang.NTRIPLES => ntriples()
      case j if lang == Lang.TURTLE => turtle
      case k if lang == Lang.RDFXML => rdfxml
      case l if lang == Lang.TRIX => trix
      // case g if lang == Lang.NQUADS => nquads(allowBlankLines)
      case _ => throw new IllegalArgumentException(s"${lang.getLabel} syntax not supported yet!")
    }

    /**
     * Loader for datasets from quad-based formats.
     *
     * @param lang
     * @return
     */
    def datasets(lang: Lang): String => RDD[JenaDataset] = {
      if (!RDFLanguages.isQuads(lang)) {
        throw new RuntimeException("Language " + lang + " not a quad-based language according to jena's registry")
      }

      if(!RDFLanguages.TRIG.equals(lang)) {
        throw new RuntimeException("Only trig format supported yet")
      }

      trig
    }

    /**
     * Load RDF data in N-Triples syntax into an [[RDD]][Triple].
     *
     * @param allowBlankLines whether blank lines will be allowed and skipped during parsing
     * @return the [[RDD]] of triples
     */
    def ntriples(allowBlankLines: Boolean = false): String => RDD[Triple] = path => {
      NTripleReader.load(spark, path)
    }

    /**
     * Load RDF data in N-Quads syntax into an [[RDD]][Triple], i.e. the graph will be omitted.
     *
     * @param allowBlankLines whether blank lines will be allowed and skipped during parsing
     * @return the [[RDD]] of quads
     */
    def nquads(allowBlankLines: Boolean = false): String => RDD[Quad] = path => {
      NQuadReader.load(spark, path)
    }

    /**
     * Load RDF data in RDF/XML syntax into an [[RDD]][Triple].
     *
     * Note, the data will not be splitted and only loaded via a single task because of the nature of XML and
     * how Spark can handle this format.
     *
     * @return the [[RDD]] of triples
     */
    def rdfxml: String => RDD[Triple] = path => {
      val confHadoop = org.apache.hadoop.mapreduce.Job.getInstance().getConfiguration
      confHadoop.setBoolean("sansa.rdf.parser.skipinvalid", true)
      confHadoop.setInt("sansa.rdf.parser.numthreads", 4)

      spark.sparkContext.newAPIHadoopFile(
        path, classOf[RiotFileInputFormat], classOf[LongWritable], classOf[Triple], confHadoop)
        .map { case (_, v) => v }
    }

    /**
     * Load RDF data in Turtle syntax into an [[RDD]][Triple]
     * @return the [[RDD]] of triples
     */
    def turtle: String => RDD[Triple] = path => {
      val confHadoop = spark.sparkContext.hadoopConfiguration

      // 1. parse the Turtle file into an RDD[String] with each entry containing a full Turtle snippet
      val rdd = spark.sparkContext.newAPIHadoopFile(
        path, classOf[TurtleInputFormat], classOf[LongWritable], classOf[TripleWritable], confHadoop)
        .map { case (_, v) => v.get() }

      rdd
    }

    /**
     * Load RDF data in TRIX syntax into an [[RDD]][Triple]
     * @return the [[RDD]] of triples
     */
    def trix: String => RDD[Triple] = path => {
      val confHadoop = org.apache.hadoop.mapreduce.Job.getInstance().getConfiguration
      confHadoop.setBoolean("sansa.rdf.parser.skipinvalid", true)
      confHadoop.set("stream.recordreader.begin", "<triple>")
      confHadoop.set("stream.recordreader.end", "</triple>")

      spark.sparkContext.newAPIHadoopFile(
        path, classOf[RiotFileInputFormat], classOf[LongWritable], classOf[Triple], confHadoop)
        .map { case (_, v) => v }
    }

    /**
     * Load RDF data in Trig syntax into an [[RDD]][Dataset]
     *
     * @return the [[RDD]] of datasets
     */
    def trig: String => RDD[JenaDataset] = path => {
      val confHadoop = spark.sparkContext.hadoopConfiguration

      spark.sparkContext.newAPIHadoopFile(path,
        classOf[TrigFileInputFormat],
        classOf[LongWritable],
        classOf[JenaDataset], confHadoop)
        .map { case (_, v) => v }
    }

  }
}
