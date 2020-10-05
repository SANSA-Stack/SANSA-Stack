package net.sansa_stack.rdf.common.io.hadoop

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.util

import org.apache.commons.io.input.BoundedInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.compress.{CodecPool, CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.jena.query.{Dataset, DatasetFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat}

/**
 * A Hadoop file input format for Trig RDF files.
 *
 * @author Lorenz Buehmann
 * @author Claus Stadler
 */
class TrigFileInputFormat
  extends FileInputFormat[LongWritable, Dataset] { // TODO use CombineFileInputFormat?

  private val logger = com.typesafe.scalalogging.Logger(this.getClass.getName)

  val PREFIXES = "prefixes"
  val PARSED_PREFIXES_LENGTH = "mapreduce.input.trigrecordreader.prefixes.maxlength"
  val PARSED_PREFIXES_LENGTH_DEFAULT = 10 * 1024 // 10KB

  override def isSplitable(context: JobContext, file: Path): Boolean = {
    val codec = new CompressionCodecFactory(context.getConfiguration).getCodec(file)
    if (null == codec) return true
    codec.isInstanceOf[SplittableCompressionCodec]
  }

  override def createRecordReader(inputSplit: InputSplit,
                                  context: TaskAttemptContext): RecordReader[LongWritable, Dataset] = {
    if (context.getConfiguration.get(PREFIXES) == null) {
      Console.err.println("couldn't get prefixes from Job context")
    }
    new TrigRecordReader()
  }

  override def getSplits(job: JobContext): util.List[InputSplit] = {
    val splits = super.getSplits(job)

    // we use first split and scan for prefixes and base IRI, then pass those to the RecordReader
    // in createRecordReader() method
    if (!splits.isEmpty) {

      // take first split
      val firstSplit = splits.get(0).asInstanceOf[FileSplit]

      // open input stream from split
      val is = getStreamFromSplit(firstSplit, job.getConfiguration)

      // we do two steps here:
      // 1. get all lines with base or prefix declaration
      // 2. use a proper parser on those lines to cover corner case like multiple prefix declarations in a single line
      val prefixStr = scala.io.Source.fromInputStream(is).getLines()
        .map(_.trim)
        .filterNot(_.isEmpty) // skip empty lines
        .filterNot(_.startsWith("#")) // skip comments
        .filter(line => line.startsWith("@prefix") || line.startsWith("@base") ||
          line.startsWith("prefix") || line.startsWith("base"))
        .mkString("\n")
      // TODO apparently, prefix declarations could span multiple lines, i.e. technically we
      //  also should consider the next line after a prefix declaration

      val dataset = DatasetFactory.create()
      RDFDataMgr.read(dataset, new ByteArrayInputStream(prefixStr.getBytes), Lang.TRIG)

      logger.info(s"parsed ${dataset.getDefaultModel.getNsPrefixMap.size()} prefixes from first" +
        s" ${job.getConfiguration.getLong(PARSED_PREFIXES_LENGTH, PARSED_PREFIXES_LENGTH_DEFAULT)} bytes")

      // prefixes are located in default model
      val baos = new ByteArrayOutputStream()
      RDFDataMgr.write(baos, dataset.getDefaultModel, RDFFormat.TURTLE_PRETTY)

      // pass prefix string to job context object
      job.getConfiguration.set("prefixes", baos.toString("UTF-8"))
    }

    splits
  }

  private def getStreamFromSplit(split: FileSplit, job: Configuration): InputStream = {
    val file = split.getPath

    // open the file and seek to the start of the split
    val fs = file.getFileSystem(job)
    val fileIn = fs.open(file)

    val start = split.getStart
    var end = start + split.getLength

    val maxPrefixesBytes = job.getLong(PARSED_PREFIXES_LENGTH, PARSED_PREFIXES_LENGTH_DEFAULT)
    if (maxPrefixesBytes > end) {
      logger.warn(s"Number of bytes set for prefixes parsing ($maxPrefixesBytes) larger than the size of the first" +
        s" split ($end). Could be slow")
    }
    end = end max maxPrefixesBytes

    val codec = new CompressionCodecFactory(job).getCodec(file)

    if (null != codec) {
      val decompressor = CodecPool.getDecompressor(codec)

      codec match {
        case splitableCodec: SplittableCompressionCodec =>
          splitableCodec.createInputStream(fileIn, decompressor, start, end, SplittableCompressionCodec.READ_MODE.BYBLOCK)
        case _ =>
          codec.createInputStream(fileIn, decompressor)
      }
    } else {
      new BoundedInputStream(fileIn, split.getLength)
    }
  }

}
