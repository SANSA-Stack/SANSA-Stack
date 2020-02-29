package net.sansa_stack.rdf.common.io.hadoop

import java.util

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{CombineFileInputFormat, FileInputFormat}
import org.apache.jena.query.Dataset

/**
 * A Hadoop file input format for Trig RDF files.
 *
 * @author Lorenz Buehmann
 */
class TrigFileInputFormat extends CombineFileInputFormat[LongWritable, Dataset] {

  override def isSplitable(context: JobContext, file: Path): Boolean = true

  override def createRecordReader(inputSplit: InputSplit,
                                  taskAttemptContext: TaskAttemptContext): RecordReader[LongWritable, Dataset] = {
    new TrigRecordReader()
  }

  override def getSplits(job: JobContext): util.List[InputSplit] = {
    val splits = super.getSplits(job)

    // TODO use first split and scan for prefixes, then pass those to the RecordReader

    splits
  }
}
