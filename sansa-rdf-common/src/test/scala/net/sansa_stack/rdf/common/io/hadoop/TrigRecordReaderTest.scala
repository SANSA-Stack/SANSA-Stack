package net.sansa_stack.rdf.common.io.hadoop

import java.io.File

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.jena.query.Dataset
import org.scalatest.FunSuite

/**
 * @author Lorenz Buehmann
 */
class TrigRecordReaderTest extends FunSuite {

  val conf = new Configuration(false)
  conf.set("fs.defaultFS", "file:///")
  val testFile = new File("/tmp/test.trig")
  val path = new Path(testFile.getAbsoluteFile.toURI)

  val length = testFile.length()

  test("single split") {

    val split = new FileSplit(path, 0, length, null)
    // setup
    val context = new TaskAttemptContextImpl(conf, new TaskAttemptID())
    val reader = new TrigRecordReader()
    // initialize
    reader.initialize(split, context)
    // read all records in split
    val actual = new mutable.ListBuffer[(LongWritable, Dataset)]()
    while (reader.nextKeyValue()) {
      val k = reader.getCurrentKey
      val v = reader.getCurrentValue
      val item = (k, v)
      println(item._2)
      actual += item
    }
  }

  test("multiple splits") {

    val nrOfSplits = 2
    var splitLength = Math.ceil(length.toDouble / nrOfSplits).toInt
    var currentLength = 0

    while (currentLength <= length) {
      val split = new FileSplit(path, 0, testFile.length(), null)
      // setup
      val context = new TaskAttemptContextImpl(conf, new TaskAttemptID())
      val reader = new TrigRecordReader()
      // initialize
      reader.initialize(split, context)
      // read all records in split
      val actual = new mutable.ListBuffer[(LongWritable, Dataset)]()
      while (reader.nextKeyValue()) {
        val k = reader.getCurrentKey
        val v = reader.getCurrentValue
        val item = (k, v)
        actual += item
      }
    }

  }

}
