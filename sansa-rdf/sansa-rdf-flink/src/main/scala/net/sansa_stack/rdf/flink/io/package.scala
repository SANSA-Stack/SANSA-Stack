package net.sansa_stack.rdf.flink

import java.io.ByteArrayOutputStream
import java.lang
import java.util.Collections

import net.sansa_stack.rdf.benchmark.io.ReadableByteChannelFromIterator
import net.sansa_stack.rdf.common.io.hadoop.RiotFileInputFormat
import net.sansa_stack.rdf.flink.io.nquads.NQuadsReader
import net.sansa_stack.rdf.flink.io.ntriples.NTriplesReader
import org.apache.flink.api.common.functions.RichMapPartitionFunction
import org.apache.flink.api.java.operators.DataSink
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.hadoopcompatibility.scala.HadoopInputs
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.sparql.core.Quad

/**
 * Wrap up implicit classes/methods to read/write RDF data from N-Triples, N-Quads or Turtle files into a
 * [[DataSet]].
 */
package object io {

  object Lang extends Enumeration {
    val NTRIPLES, NQUADS, TURTLE, RDFXML = Value
  }

  /**
   * Adds methods, `ntriples` and `turtle`, to [[org.apache.flink.api.scala.ExecutionEnvironment]] that allows to
   * write N-Triples and N-Quads files.
   */
  implicit class RDFWriter[T](ds: DataSet[Triple]) {

    /**
     * Writes the triples as N-Triples file(s) to the specified location.
     *
     * <ul>
     * <li>
     * A directory is created and multiple files are written underneath. (Default behavior)<br/>
     * This sink creates a directory called "path1", and files "1", "2" ... are writen underneath depending
     * on <a href="https://flink.apache.org/faq.html#what-is-the-parallelism-how-do-i-set-it">parallelism</a>
     *
     * <br/>
     * {{{
     * .
     * └── path1/
     *     ├── 1
     *     ├── 2
     *     └── ...
     * }}}
     *
     * Code Example
     * {{{
     * dataset.saveAsNTriplesFile("file:///path1")
     * }}}
     * </li>
     *
     * <li>
     * A single file called "path1" is created when parallelism is set to 1
     * {{{
     * .
     * └── path1
     * }}}
     *
     * Code Example
     * {{{
     * // Parallelism is set to only this particular operation
     * dataset.saveAsNTriplesFile("file:///path1").setParallelism(1)
     *
     * // This will have the same effect but note all operators' parallelism are set to one
     * env.setParallelism(1);
     * ...
     * dataset.saveAsNTriplesFile("file:///path1")
     * }}}
     * </li>
     * <li>
     * A directory is always created when <a href="https://ci.apache.org/projects/flink/flink-docs-master/setup/config.html#file-systems">fs.output.always-create-directory</a>
     * is set to true in flink-conf.yaml file, even when parallelism is set to 1.
     * {{{
     * .
     * └── path1/
     *     └── 1
     * }}}
     *
     * Code Example
     * {{{
     * // fs.output.always-create-directory = true
     * dataset.saveAsNTriplesFile("file:///path1").setParallelism(1)
     * }}}
     * </li>
     * </ul>
     *
     * @param path The path pointing to the location the text file or files under the directory is written to.
     * @param writeMode Control the behavior for existing files. Options are NO_OVERWRITE and OVERWRITE.
     * @return The DataSink that writes the DataSet.
     */
    def saveAsNTriplesFile(
      path: String,
      writeMode: FileSystem.WriteMode = FileSystem.WriteMode.NO_OVERWRITE): DataSink[String] = {

      import scala.jdk.CollectionConverters._

      ds
        .mapPartition(p => {
          val os = new ByteArrayOutputStream()
          RDFDataMgr.writeTriples(os, p.asJava)
          Collections.singleton(new String(os.toByteArray)).iterator().asScala
        })
        .writeAsText(path)
    }

    /**
     * Writes the triples as N-Quads file(s) to the specified location using the given graph.
     *
     * <ul>
     * <li>
     * A directory is created and multiple files are written underneath. (Default behavior)<br/>
     * This sink creates a directory called "path1", and files "1", "2" ... are writen underneath depending
     * on <a href="https://flink.apache.org/faq.html#what-is-the-parallelism-how-do-i-set-it">parallelism</a>
     *
     * <br/>
     * {{{
     * .
     * └── path1/
     *     ├── 1
     *     ├── 2
     *     └── ...
     * }}}
     *
     * Code Example
     * {{{
     * dataset.saveAsNQuadsFile("file:///path1")
     * }}}
     * </li>
     *
     * <li>
     * A single file called "path1" is created when parallelism is set to 1
     * {{{
     * .
     * └── path1
     * }}}
     *
     * Code Example
     * {{{
     * // Parallelism is set to only this particular operation
     * dataset.saveAsNQuadsFile("file:///path1").setParallelism(1)
     *
     * // This will have the same effect but note all operators' parallelism are set to one
     * env.setParallelism(1);
     * ...
     * dataset.saveAsNQuadsFile("file:///path1")
     * }}}
     * </li>
     * <li>
     * A directory is always created when <a href="https://ci.apache.org/projects/flink/flink-docs-master/setup/config.html#file-systems">fs.output.always-create-directory</a>
     * is set to true in flink-conf.yaml file, even when parallelism is set to 1.
     * {{{
     * .
     * └── path1/
     *     └── 1
     * }}}
     *
     * Code Example
     * {{{
     * // fs.output.always-create-directory = true
     * dataset.saveAsNQuadsFile("file:///path1").setParallelism(1)
     * }}}
     * </li>
     * </ul>
     *
     * @param graph The graph used for the N-Quads
     * @param path The path pointing to the location the text file or files under the directory is written to.
     * @param writeMode Control the behavior for existing files. Options are NO_OVERWRITE and OVERWRITE.
     * @return The DataSink that writes the DataSet.
     */
    def saveAsNQuadsFile(
      graph: String,
      path: String,
      writeMode: FileSystem.WriteMode = FileSystem.WriteMode.NO_OVERWRITE): DataSink[String] = {

      import scala.jdk.CollectionConverters._

      ds
        .mapPartition(p => {
          // map to N-Quads
          val g = NodeFactory.createURI(graph)
          val nquadsIt = p.map(Quad.create(g, _)).asJava

          // write to string object
          val os = new ByteArrayOutputStream()
          RDFDataMgr.writeQuads(os, nquadsIt)
          Collections.singleton(new String(os.toByteArray)).iterator().asScala
        })
        .writeAsText(path)

    }
  }

  /**
   * Adds methods, `rdf(lang: Lang)`, `ntriples`, `nquads`, and `turtle`, to [[ExecutionEnvironment]] that allows to read
   * N-Triples, N-Quads and Turtle files.
   */
  implicit class RDFReader(env: ExecutionEnvironment) {

    import scala.jdk.CollectionConverters._

    /**
     * Load RDF data into an [[org.apache.flink.api.scala.DataSet]][Triple]. Currently, N-Triples, N-Quads and Turtle syntax are supported.
     * @param lang the RDF language (N-Triples, N-Quads, Turtle)
     * @return the [[DataSet]]
     */
    def rdf(lang: Lang.Value, allowBlankLines: Boolean = false): String => DataSet[Triple] = lang match {
      case i if lang == Lang.NTRIPLES => ntriples(allowBlankLines)
      case j if lang == Lang.TURTLE => turtle
      case k if lang == Lang.RDFXML => rdfxml
      case g if lang == Lang.NQUADS => nquads(allowBlankLines)
      case _ => throw new IllegalArgumentException(s"${lang} syntax not supported yet!")
    }

    /**
     * Load RDF data in N-Triples syntax into an [[DataSet]][Triple].
     *
     * @param allowBlankLines whether blank lines will be allowed and skipped during parsing
     * @return the [[DataSet]] of triples
     */
    def ntriples(allowBlankLines: Boolean = false): String => DataSet[Triple] = path => {
      NTriplesReader.load(env, path)
    }

    /**
     * Load RDF data in N-Quads syntax into an [[DataSet]][Triple], i.e. the graph will be omitted.
     *
     * @param allowBlankLines whether blank lines will be allowed and skipped during parsing
     * @return the [[DataSet]] of triples
     */
    def nquads(allowBlankLines: Boolean = false): String => DataSet[Triple] = path => {
      NQuadsReader.load(env, path)
    }

    /**
     * Load RDF data in RDF/XML syntax into an [[DataSet]][Triple].
     *
     * Note, the data will not be splitted and only loaded via a single task because of the nature of XML and
     * how Spark can handle this format.
     *
     * @return the [[DataSet]] of triples
     */
    def rdfxml: String => DataSet[Triple] = path => {
      val job = org.apache.hadoop.mapreduce.Job.getInstance()

      val confHadoop = job.getConfiguration
      confHadoop.setBoolean("sansa.rdf.parser.skipinvalid", true)
      confHadoop.setInt("sansa.rdf.parser.numthreads", 4)

      val wrappedFormat = HadoopInputs.readHadoopFile(
        new RiotFileInputFormat,
        classOf[LongWritable],
        classOf[Triple],
        path,
        job)

      env.createInput(wrappedFormat).map(_._2)
    }

    import org.apache.flink.api.scala.extensions._

    /**
     * Load RDF data in Turtle syntax into an [[DataSet]][Triple]
     * @return the [[DataSet]] of triples
     */
    def turtle: String => DataSet[Triple] = path => {
      val job = org.apache.hadoop.mapreduce.Job.getInstance()

      val confHadoop = job.getConfiguration
      confHadoop.set("textinputformat.record.delimiter", ".\n")

      val wrappedFormat = HadoopInputs.readHadoopFile(
        new TextInputFormat,
        classOf[LongWritable],
        classOf[Text],
        path,
        job)

      // 1. parse the Turtle file into an RDD[String] with each entry containing a full Turtle snippet
      val turtleRDD = env.createInput(wrappedFormat)
        .filter(!_._2.toString.trim.isEmpty)
        .mapWith { case (_, v) => v.toString.trim }

      //      turtleRDD.collect().foreach(chunk => println("Chunk" + chunk))

      // 2. we need the prefixes - two options:
      // a) assume that all prefixes occur in the beginning of the document
      // b) filter all lines that contain the prefixes
      val prefixes = turtleRDD.filter(_.startsWith("@prefix"))

      // we broadcast the prefixes
      turtleRDD.mapPartition(new RichMapPartitionFunction[String, Triple]() {
        var prefixes: java.util.List[String] = _

        override def open(parameters: Configuration): Unit = {
          prefixes = getRuntimeContext.getBroadcastVariable[String]("prefixes")
        }

        override def mapPartition(iterable: lang.Iterable[String], collector: Collector[Triple]): Unit = {
          // concat prefixes and Turtle snippet
          val it = (prefixes.asScala ++ iterable.asScala).asJava.iterator()

          // collect the parsed triples
          val is = ReadableByteChannelFromIterator.toInputStream(it)
          RDFDataMgr
            .createIteratorTriples(is, org.apache.jena.riot.Lang.TURTLE, null).asScala
            .foreach(t => collector.collect(t))
        }
      })
        .withBroadcastSet(prefixes, "prefixes")
    }
  }
}
