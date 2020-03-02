package net.sansa_stack.rdf.common.io.hadoop

import java.io.{ByteArrayInputStream, File, FileInputStream}

import org.aksw.commons.collections.diff.{CollectionDiff, Diff, SetDiff}
import org.aksw.jena_sparql_api.core.utils.FN_QuadDiffUnique
import org.aksw.jena_sparql_api.update.QuadDiffIterator

import scala.collection.mutable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, RecordReader, TaskAttemptID}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.jena.ext.com.google.common.collect.Sets
import org.apache.jena.query.{Dataset, DatasetFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat}
import org.apache.jena.sparql.core.Quad
import org.scalatest.FunSuite

import scala.collection.JavaConverters._
import org.apache.jena.sparql.util.compose.DatasetLib

/**
 * @author Lorenz Buehmann
 */
class TrigRecordReaderTest extends FunSuite {

  val conf = new Configuration(false)
  conf.set("fs.defaultFS", "file:///")

  // val testFileName = "w3c_ex2.trig"
  val testFileName = "w3c_ex2-no-default-graph.trig"
  val testFile = new File(getClass.getClassLoader.getResource(testFileName).getPath)

  val path = new Path(testFile.getAbsolutePath)

  val fileLengthTotal = testFile.length()

  // read the target dataset
  val targetDataset = DatasetFactory.create()
  RDFDataMgr.read(targetDataset, new FileInputStream(testFile), Lang.TRIG)


  val maxNumSplits = 2

  /**
   * Testing n splits by manually created RecordReader
   */
  for (i <- maxNumSplits to maxNumSplits) {
    test(s"parsing Trig file provided by $i splits") {

      val splits = generateFileSplits(i)

      splits.foreach { split =>
        // println(s"split (${split.getStart} - ${split.getStart + split.getLength}):" )

        val stream = split.getPath.getFileSystem(new TaskAttemptContextImpl(conf, new TaskAttemptID()).getConfiguration)
          .open(split.getPath)


        val bufferSize = split.getLength.toInt
        val buffer = new Array[Byte](bufferSize)
        stream.readFully(split.getStart, buffer, 0, bufferSize)
        println(new String(buffer))
        stream.close()

        // setup
        val reader = new TrigRecordReader()

        // initialize
        reader.initialize(split, new TaskAttemptContextImpl(conf, new TaskAttemptID()))

        // read all records in split
        val ds = consumeRecords(reader)

        System.err.println("Consumed dataset")
        RDFDataMgr.write(System.err, ds, RDFFormat.TRIG_PRETTY)

        // compare with target dataset
        val isEqual = compareDatasets(targetDataset, ds)
        assert(isEqual, "datasets did not match")
      }
    }
  }

  /**
   * Testing n splits by RecordReader created from Inputformat (incl. parsed prefixes)
   */
  /*
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
      val ds = consumeRecords(reader)

      // compare with target dataset
      compareDatasets(targetDataset, ds)
    }
  }
  */

  private def consumeRecords(reader: RecordReader[LongWritable, Dataset]): Dataset = {
    val result = DatasetFactory.create
    // val actual = new mutable.ListBuffer[(LongWritable, Dataset)]()
    while (reader.nextKeyValue()) {
      val k = reader.getCurrentKey
      val v = reader.getCurrentValue
      val item = (k, v)
      // actual += item
      // println(s"Dataset ${k.get()}:")
      // RDFDataMgr.write(System.out, v, RDFFormat.TRIG_PRETTY)

      for (q <- v.asDatasetGraph().find().asScala) {
        result.asDatasetGraph().add(q)
      }
    }

    // merge to single dataset
    // actual.map(_._2).foldLeft(DatasetFactory.create())((ds1, ds2) => DatasetLib.union(ds1, ds2))
    result
  }

  private def generateFileSplits(n: Int) = {
    val splitLength = Math.ceil(fileLengthTotal.toDouble / n).toInt

    for (i <- 0 until n) yield {
      val start = i * splitLength
      val end = Math.min((i + 1) * splitLength, fileLengthTotal)
      val length = end - start

      new FileSplit(path, start, length, null)
    }
  }


  private def compareDatasets(ds1: Dataset, ds2: Dataset): Boolean = {

    /*

    val a = Sets.newHashSet(ds1.asDatasetGraph().find())
    val b = Sets.newHashSet(ds2.asDatasetGraph().find())

    val diff = new SetDiff[Quad](a, b)

    System.err.println("Excessive")
    for(x <- diff.getAdded.asScala) {
      System.err.println("  " + x)
    }

    System.err.println("Missing")
    for(x <- diff.getRemoved.asScala) {
      System.err.println("  " + x)
    }

    System.err.println("Report done")
    */

    /*
    System.err.println("Dataset 1")
    RDFDataMgr.write(System.err, ds1, RDFFormat.TRIG_PRETTY)
    System.err.println("Dataset 2")
    RDFDataMgr.write(System.err, ds2, RDFFormat.TRIG_PRETTY)
    System.err.println("Datasets printed")
    */

    // compare default graphs first
    if (!ds1.getDefaultModel.getGraph.isIsomorphicWith(ds2.getDefaultModel.getGraph)) {
      false
    } else { // then compare the named graphs TODO this doesn't handle blank node graph names
      val allNames = Sets.union(
        ds1.listNames().asScala.toSet.asJava,
        ds2.listNames().asScala.toSet.asJava).asScala

      allNames.forall(g => {
        val c1 = ds1.containsNamedModel(g)
        val c2 = ds2.containsNamedModel(g)
        var r = false
        if(c1 && c2) {
          val g1 = ds1.getNamedModel(g).getGraph
          val g2 = ds2.getNamedModel(g).getGraph
          val g1s = g1.size
          val g2s = g2.size
          r = g1.isIsomorphicWith(g2)
        }
        r
      })
    }
  }


}
