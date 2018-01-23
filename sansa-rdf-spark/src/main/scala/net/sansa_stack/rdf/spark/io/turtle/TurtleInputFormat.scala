package net.sansa_stack.rdf.spark.io.turtle

import com.google.common.base.Charsets
import net.sansa_stack.rdf.common.annotation.Experimental
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

/**
  * An Hadoop input format for Turtle data.
  *
  * @author Lorenz Buehmann
  */
@Experimental
class TurtleInputFormat extends TextInputFormat {

  override def createRecordReader(
                                   split: InputSplit,
                                   context: TaskAttemptContext): RecordReader[LongWritable, Text] = {
    val delimiter = context.getConfiguration.get("textinputformat.record.delimiter")
    val recordDelimiterBytes: Array[Byte] = if (delimiter == null) null else delimiter.getBytes(Charsets.UTF_8)

    new TurtleRecordReader(recordDelimiterBytes)
  }

}
