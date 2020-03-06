package net.sansa_stack.rdf.common.io.hadoop

import java.io.{ByteArrayInputStream, File, FileInputStream}

import org.aksw.commons.collections.diff.{CollectionDiff, Diff, SetDiff}
import org.aksw.jena_sparql_api.core.utils.FN_QuadDiffUnique
import org.aksw.jena_sparql_api.update.QuadDiffIterator
import org.aksw.jena_sparql_api.utils.DatasetGraphUtils
import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, Job, RecordReader, TaskAttemptID}
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
  conf.set(TrigRecordReader.MAX_RECORD_LENGTH, "10000")
  conf.set(TrigRecordReader.PROBE_RECORD_COUNT, "1")

  // val testFileName = "w3c_ex2.trig"
  val testFileName = "w3c_ex2-no-default-graph.trig"
  // val testFile = new File("/home/raven/Projects/Eclipse/sparql-integrate-parent/tmp/unique-queries.one-week.trig")
  val testFile = new File(getClass.getClassLoader.getResource(testFileName).getPath)

  val path = new Path(testFile.getAbsolutePath)

  val fileLengthTotal = testFile.length()

  // read the target dataset
  val targetDataset = DatasetFactory.create()
  RDFDataMgr.read(targetDataset, new FileInputStream(testFile), Lang.TRIG)


  val maxNumSplits = 4

  val job = Job.getInstance(conf)
  val inputFormat = new TrigFileInputFormat()

  // add input path of the file
  FileInputFormat.addInputPath(job, new Path(testFile.getAbsolutePath))

  // call once to compute the prefixes
  inputFormat.getSplits(job)

  /**
   * Testing n splits by manually created RecordReader
   */
  for (i <- 1 to maxNumSplits) {
    test(s"parsing Trig file provided by $i splits") {

      val splits = generateFileSplits(i)

      val ds = DatasetFactory.create()
      splits.foreach { split =>
        // println(s"split (${split.getStart} - ${split.getStart + split.getLength}):" )

        /*
                val stream = split.getPath.getFileSystem(new TaskAttemptContextImpl(conf, new TaskAttemptID()).getConfiguration)
                  .open(split.getPath)

                val bufferSize = split.getLength.toInt
                val buffer = new Array[Byte](bufferSize)
                stream.readFully(split.getStart, buffer, 0, bufferSize)
                println(new String(buffer))
                stream.close()
                */

        // setup
        val reader = inputFormat.createRecordReader(split, new TaskAttemptContextImpl(job.getConfiguration, new TaskAttemptID()))
//        val reader = new TrigRecordReader()

        // initialize
        reader.initialize(split, new TaskAttemptContextImpl(job.getConfiguration, new TaskAttemptID()))

        // read all records in split
        val contrib = consumeRecords(reader)
        DatasetGraphUtils.addAll(ds.asDatasetGraph(), contrib.asDatasetGraph())
//        System.err.println("Dataset contribution")
//        RDFDataMgr.write(System.err, ds, RDFFormat.TRIG_PRETTY)
      }

      // compare with target dataset
      compareDatasets(targetDataset, ds)
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

      DatasetGraphUtils.addAll(result.asDatasetGraph(), v.asDatasetGraph())
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

      new FileSplit(path, start, length, null).asInstanceOf[InputSplit]
    }
  }


  private def compareDatasets(ds1: Dataset, ds2: Dataset): Unit = {

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
    assert(ds1.getDefaultModel.getGraph.isIsomorphicWith(ds2.getDefaultModel.getGraph),
      "default graphs do not match")

    // then compare the named graphs
    val allNames = (ds1.listNames().asScala ++ ds2.listNames().asScala).toSet

    allNames.foreach(g => {
      assert(ds1.containsNamedModel(g), s"graph <$g> not found in first dataset")
      assert(ds2.containsNamedModel(g), s"graph <$g> not found in second dataset")

      val g1 = ds1.getNamedModel(g).getGraph
      val g2 = ds2.getNamedModel(g).getGraph

      assert(g1.size == g2.size, s"size of graph <$g> not the same in both datasets")
      // Isomorphism check may fail with stack overflow execution if datasets
      // become too large
      // assert(g1.isIsomorphicWith(g2), s"graph <$g> not isomorph")

    })
  }


}
