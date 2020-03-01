package net.sansa_stack.rdf.common.io.hadoop

import java.io.File

import scala.collection.mutable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, RecordReader, TaskAttemptID}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.jena.query.Dataset
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.scalatest.FunSuite

import scala.collection.JavaConverters._

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


  val maxNumSplits = 3

  /**
   * Testing n splits by manually created RecordReader
   */
  for (i <- 1 to maxNumSplits) {
    test(s"parsing Trig file provided by $i splits") {

      val splits = generateFileSplits(i)

      splits.foreach { split =>
        // setup
        val reader = new TrigRecordReader()

        // initialize
        reader.initialize(split, new TaskAttemptContextImpl(conf, new TaskAttemptID()))

        // read all records in split
        consumeRecords(reader)
      }
    }
  }

  /**
   * Testing n splits by RecordReader created from Inputformat (inkl. parsed prefixes)
   */
  test("multiple splits parsed using InputFormat") {

    val job = Job.getInstance(conf)
    val inputFormat = new TrigFileInputFormat()

    // add input path of the file
    FileInputFormat.addInputPath(job, new Path(testFile.getAbsolutePath))

    // get splits from InputFormat
    val splits = inputFormat.getSplits(job)

    splits.asScala.foreach { split =>
      // create the reader
      val reader = inputFormat.createRecordReader(split, new TaskAttemptContextImpl(conf, new TaskAttemptID()))

      // read all records in split
      consumeRecords(reader)
    }
  }

  private def consumeRecords(reader: RecordReader[LongWritable, Dataset]) = {
    val actual = new mutable.ListBuffer[(LongWritable, Dataset)]()
    while (reader.nextKeyValue()) {
      val k = reader.getCurrentKey
      val v = reader.getCurrentValue
      val item = (k, v)
      actual += item
      println(s"Graph ${k.get()}:")
      RDFDataMgr.write(System.out, v, RDFFormat.TRIG_PRETTY)
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
