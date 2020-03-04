package net.sansa_stack.rdf.common.io.hadoop

import java.io.{ByteArrayInputStream, FileInputStream}
import java.util

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{CombineFileInputFormat, CombineFileSplit, FileInputFormat, FileSplit}
import org.apache.jena.query.{Dataset, DatasetFactory}
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.{Lang, RDFDataMgr}

import scala.collection.JavaConverters._

/**
 * A Hadoop file input format for Trig RDF files.
 *
 * @author Lorenz Buehmann
 */
class TrigFileInputFormat
  extends FileInputFormat[LongWritable, Dataset] { // TODO use CombineFileInputFormat?

  var prefixMapping: Model = _

  override def isSplitable(context: JobContext, file: Path): Boolean = true

  override def createRecordReader(inputSplit: InputSplit,
                                  context: TaskAttemptContext): RecordReader[LongWritable, Dataset] = {
    val maxRecordLength = Option(context.getConfiguration.get("trig.record.maxLength")).getOrElse("200").toInt
    new TrigRecordReader(prefixMapping, maxRecordLength)
  }

  override def getSplits(job: JobContext): util.List[InputSplit] = {
    val splits = super.getSplits(job)

    // we use first split and scan for prefixes and base IRI, then pass those to the RecordReader
    // in createRecordReader() method
    if (!splits.isEmpty) {
      val firstSplit = splits.get(0).asInstanceOf[FileSplit]

      val dataset = DatasetFactory.create()

      val is = firstSplit.getPath.getFileSystem(job.getConfiguration).open(firstSplit.getPath)
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

      RDFDataMgr.read(dataset, new ByteArrayInputStream(prefixStr.getBytes), Lang.TRIG)
      // prefixes are located in default model
      prefixMapping = dataset.getDefaultModel
    }

    splits.asScala
      .zipWithIndex
      .map { case (split, idx) =>
        IndexedInputSplit(split.asInstanceOf[FileSplit], idx, (idx + 1) == splits.size()).asInstanceOf[InputSplit] }
      .toList.asJava
  }

}

/**
 * File input split with index position of split.
 *
 * @param split the file split
 * @param idx index of split
 */
case class IndexedInputSplit(split: FileSplit, idx: Int, isLastSplit: Boolean)
  extends FileSplit(split.getPath, split.getStart, split.getLength, split.getLocations) {
}
