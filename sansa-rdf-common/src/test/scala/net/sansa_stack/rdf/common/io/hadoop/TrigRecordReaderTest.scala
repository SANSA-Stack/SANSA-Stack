package net.sansa_stack.rdf.common.io.hadoop

import java.io.File

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.jena.query.Dataset
import org.scalatest.FunSuite

/**
 * @author Lorenz Buehmann
 */
class TrigRecordReaderTest extends FunSuite {

  val conf = new Configuration(false)
  conf.set("fs.defaultFS", "file:///")

  val testFileName = "w3c_ex2.trig"
  val testFile = new File(getClass.getClassLoader.getResource(testFileName).getPath)

  val path = new Path(testFile.getAbsolutePath)

  val fileLengthTotal = testFile.length()

  test("single split") {

    val split = new FileSplit(path, 0, fileLengthTotal, null)
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

    val reader = new TrigRecordReader()

    generateFileSplits(nrOfSplits).foreach { split =>
      // setup
      val context = new TaskAttemptContextImpl(conf, new TaskAttemptID())

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

  test("multiple splits parsed using InputFormat") {

    val inputFormat = new TrigFileInputFormat()

    val nrOfSplits = 2

    val reader = new TrigRecordReader()

    generateFileSplits(nrOfSplits).foreach { split =>
      // setup
      val context = new TaskAttemptContextImpl(conf, new TaskAttemptID())

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

  private def generateFileSplits(n: Int) = {
    val splitLength = Math.ceil(fileLengthTotal.toDouble / n).toInt

    for (i <- 0 to n) yield {
      val start = i * splitLength
      val end = Math.min((i + 1) * splitLength, fileLengthTotal)

      new FileSplit(path, start, end, null)
    }
  }

}
