package net.sansa_stack.rdf.spark.io.rdfxml

import java.io.InputStream

import scala.reflect.ClassTag

import com.google.common.io.ByteStreams
import net.sansa_stack.rdf.spark.utils.ScalaUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.jena.riot.{RDFParser, RDFParserBuilder}
import org.apache.spark.TaskContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{CodecStreams, HadoopFileLinesReader, PartitionedFile}
import org.apache.spark.unsafe.types.UTF8String




/**
  * Common functions for parsing RDF/XML files
 *
  * @tparam T A datatype containing the unparsed RDF/XML, such as [[Text]] or [[String]]
  * @author Lorenz Buehmann
  */
abstract class RdfXmlDataSource[T] extends Serializable {
  def isSplitable: Boolean

  /**
    * Parse a [[PartitionedFile]] into 0 or more [[InternalRow]] instances
    */
  def readFile(
                conf: Configuration,
                file: PartitionedFile,
                parser: JenaParser): Iterator[InternalRow]

  /**
    * Create an [[RDD]] that handles the preliminary parsing of [[T]] records
    */
  protected def createBaseRdd(
                               sparkSession: SparkSession,
                               inputPaths: Seq[FileStatus]): RDD[T]

  /**
    * A generic wrapper to invoke the correct [[RDFParserBuilder]] method to allocate a [[RDFParser]]
    * for an instance of [[T]]
    */
  def createParser(parserBuilder: RDFParserBuilder, value: T): RDFParser

}

object RdfXmlDataSource {
  def apply(options: RdfXmlOptions): RdfXmlDataSource[_] = {
    if (options.wholeFile) {
      WholeFileRdfXmlDataSource
    } else {
      WholeFileRdfXmlDataSource
//      TextInputRdfXmlDataSource
    }
  }

  /**
    * Create a new [[RDD]] via the supplied callback if there is at least one file to process,
    * otherwise an [[org.apache.spark.rdd.EmptyRDD]] will be returned.
    */
  def createBaseRdd[T : ClassTag](
                                   sparkSession: SparkSession,
                                   inputPaths: Seq[FileStatus])(
                                   fn: (Configuration, String) => RDD[T]): RDD[T] = {
    val paths = inputPaths.map(_.getPath)

    if (paths.nonEmpty) {
      val job = Job.getInstance(sparkSession.sessionState.newHadoopConf())
      FileInputFormat.setInputPaths(job, paths: _*)
      fn(job.getConfiguration, paths.mkString(","))
    } else {
      sparkSession.sparkContext.emptyRDD[T]
    }
  }
}

object TextInputRdfXmlDataSource extends RdfXmlDataSource[Text] {
  override val isSplitable: Boolean = {
    // splittable if the underlying source is
    false
  }

  override protected def createBaseRdd(
                                        sparkSession: SparkSession,
                                        inputPaths: Seq[FileStatus]): RDD[Text] = {
    RdfXmlDataSource.createBaseRdd(sparkSession, inputPaths) {
      case (conf, name) =>
        sparkSession.sparkContext.newAPIHadoopRDD(
          conf,
          classOf[TextInputFormat],
          classOf[LongWritable],
          classOf[Text])
          .setName(s"RdfXmlLines: $name")
          .values // get the text column
    }
  }

  override def readFile(
                         conf: Configuration,
                         file: PartitionedFile,
                         parser: JenaParser): Iterator[InternalRow] = {
    val linesReader = new HadoopFileLinesReader(file, conf)
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => linesReader.close()))
    linesReader.flatMap(parser.parse(_, createParser, textToUTF8String))
  }

  private def textToUTF8String(value: Text): UTF8String = {
    UTF8String.fromBytes(value.getBytes, 0, value.getLength)
  }

  override def createParser(parserBuilder: RDFParserBuilder, value: Text): RDFParser = {
    CreateRdfXmlParser.text(parserBuilder, value)
  }
}

object WholeFileRdfXmlDataSource extends RdfXmlDataSource[PortableDataStream] {
  override val isSplitable: Boolean = {
    false
  }

  override protected def createBaseRdd(
                                        sparkSession: SparkSession,
                                        inputPaths: Seq[FileStatus]): RDD[PortableDataStream] = {
    RdfXmlDataSource.createBaseRdd(sparkSession, inputPaths) {
      case (conf, name) =>
        sparkSession.sparkContext.binaryFiles(inputPaths.map(_.getPath).mkString(",")).setName(s"RDF/XML File: $name").values
//        new BinaryFileRDD(
//          sparkSession.sparkContext,
//          classOf[StreamInputFormat],
//          classOf[String],
//          classOf[PortableDataStream],
//          conf,
//          sparkSession.sparkContext.defaultMinPartitions)
//          .setName(s"RDF/XML File: $name")
//          .values
    }
  }

  private def createInputStream(config: Configuration, path: String): InputStream = {
    val inputStream = CodecStreams.createInputStream(config, new Path(path))
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => inputStream.close()))
    inputStream
  }

  override def createParser(parserBuilder: RDFParserBuilder, record: PortableDataStream): RDFParser = {
    CreateRdfXmlParser.inputStream(
      parserBuilder,
      createInputStream(record.getConfiguration, record.getPath()))
  }

  override def readFile(
                         conf: Configuration,
                         file: PartitionedFile,
                         parser: JenaParser): Iterator[InternalRow] = {
    def partitionedFileString(ignored: Any): UTF8String = {
      ScalaUtils.tryWithResource(createInputStream(conf, file.filePath)) { inputStream =>
        UTF8String.fromBytes(ByteStreams.toByteArray(inputStream))
      }.get // TODO handle errors
    }

    parser.parse(
      createInputStream(conf, file.filePath),
      CreateRdfXmlParser.inputStream,
      partitionedFileString).toIterator
  }
}
