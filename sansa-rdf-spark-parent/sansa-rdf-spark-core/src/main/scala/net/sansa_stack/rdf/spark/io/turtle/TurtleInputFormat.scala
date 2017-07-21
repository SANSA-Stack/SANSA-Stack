package net.sansa_stack.rdf.spark.io.turtle

import com.google.common.base.Charsets
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

/**
  * @author Lorenz Buehmann
  */
class TurtleInputFormat extends TextInputFormat {

  override def createRecordReader(
                                   split: InputSplit,
                                   context: TaskAttemptContext): RecordReader[LongWritable, Text] = {
    val delimiter = context.getConfiguration.get("textinputformat.record.delimiter")
    var recordDelimiterBytes: Array[Byte] = if (delimiter == null) null else delimiter.getBytes(Charsets.UTF_8)

    new TurtleRecordReader(recordDelimiterBytes)
  }

}
