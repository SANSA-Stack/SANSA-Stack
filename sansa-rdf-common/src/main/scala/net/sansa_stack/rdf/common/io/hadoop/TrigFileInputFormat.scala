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

/**
 * A Hadoop file input format for Trig RDF files.
 *
 * @author Lorenz Buehmann
 */
class TrigFileInputFormat extends FileInputFormat[LongWritable, Dataset] { // TODO use CombineFileInputFormat?

  var prefixMapping: Model = _

  override def isSplitable(context: JobContext, file: Path): Boolean = true

  override def createRecordReader(inputSplit: InputSplit,
                                  taskAttemptContext: TaskAttemptContext): RecordReader[LongWritable, Dataset] = {
    new TrigRecordReader(prefixMapping)
  }

  override def getSplits(job: JobContext): util.List[InputSplit] = {
    val splits = super.getSplits(job)

    // we use first split and scan for prefixes and base IRI, then pass those to the RecordReader
    // in createRecordReader() method
    if (!splits.isEmpty) {
      val firstSplit = splits.get(0).asInstanceOf[FileSplit]

      val dataset = DatasetFactory.create()

      val is = firstSplit.getPath.getFileSystem(job.getConfiguration).open(firstSplit.getPath)
      val prefixStr = scala.io.Source.fromInputStream(is).getLines()
        .map(_.trim)
        .filter(line => line.startsWith("@prefix") || line.startsWith("@base"))
        .mkString

      RDFDataMgr.read(dataset, new ByteArrayInputStream(prefixStr.getBytes), Lang.TRIG)
      prefixMapping = dataset.getUnionModel.removeAll()
    }

    splits
  }
}
