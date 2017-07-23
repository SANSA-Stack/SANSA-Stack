package net.sansa_stack.ml.spark.kernel

import java.io.File

import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleRDD
import org.apache.jena.graph
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDFFastGraphKernelApp {

  def main(args: Array[String]): Unit = {
    val taskNum: Int = scala.io.StdIn.readLine("Task Number?(1=Affiliation, 3=Multi-contract, 4=Theme) ").toInt
    val iteration: Int = scala.io.StdIn.readLine("How many iterations or folding on validation? ").toInt

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("fast graph kernel")
      .config("spark.driver.maxResultSize", "3g")
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)


    val t0 = System.nanoTime

    if (taskNum == 1) {
      experimentAffiliationPrediction(sparkSession, iteration)
    }

    if (taskNum == 3) {
      experimentMultiContractPrediction(sparkSession, iteration)
    }

    if (taskNum == 4) {
      experimentThemePrediction(sparkSession, iteration)
    }

    RDFFastTreeGraphKernelUtil.printTime("Total: ", t0, System.nanoTime)
    println("taskNum: " + taskNum)
    println("iteration: " + iteration)
    sparkSession.stop
  }

  def experimentAffiliationPrediction(sparkSession: SparkSession, iteration: Int): Unit = {
    val input = "src/main/resources/kernel/aifb-fixed_no_schema4.nt"

    val t0 = System.nanoTime

    val triples: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input))
      .filter(_.getPredicate.getURI != "http://swrc.ontoware.org/ontology#employs")
    val tripleRDD: TripleRDD = new TripleRDD(triples)

    val rdfFastGraphKernel = RDFFastGraphKernel(sparkSession, tripleRDD, "http://swrc.ontoware.org/ontology#affiliation")
    val data = rdfFastGraphKernel.getMLLibLabeledPoints

    val t1 = System.nanoTime
    RDFFastTreeGraphKernelUtil.printTime("Initialization", t0, t1)

    RDFFastTreeGraphKernelUtil.predictLogisticRegressionMLLIB(data,4,iteration)

    val t2 = System.nanoTime
    RDFFastTreeGraphKernelUtil.printTime("Run Prediction", t1, t2)

  }


  def experimentMultiContractPrediction(sparkSession: SparkSession, iteration: Int): Unit = {
    val input = "src/main/resources/kernel/LDMC_Task2_train.nt"

    val t0 = System.nanoTime

    val triples: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input))
    val tripleRDD: TripleRDD = new TripleRDD(triples)


    val rdfFastGraphKernel = RDFFastGraphKernel(sparkSession, tripleRDD, "http://example.com/multicontract")
    val data = rdfFastGraphKernel.getMLLibLabeledPoints

    val t1 = System.nanoTime
    RDFFastTreeGraphKernelUtil.printTime("Initialization", t0, t1)

    RDFFastTreeGraphKernelUtil.predictLogisticRegressionMLLIB(data,2,iteration)

    val t2 = System.nanoTime
    RDFFastTreeGraphKernelUtil.printTime("Run Prediction", t1, t2)

  }


  def experimentThemePrediction(sparkSession: SparkSession, iteration: Int, fraction: String = ""): Unit = {

    val input = "src/main/resources/kernel/Lexicon_NamedRockUnit_t" + fraction + ".nt"

    val t0 = System.nanoTime

    val triples: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input))
    val tripleRDD: TripleRDD = new TripleRDD(triples)

    val rdfFastGraphKernel = RDFFastGraphKernel(sparkSession, tripleRDD, "http://data.bgs.ac.uk/ref/Lexicon/hasTheme")
    val data = rdfFastGraphKernel.getMLLibLabeledPoints

    val t1 = System.nanoTime
    RDFFastTreeGraphKernelUtil.printTime("Initialization", t0, t1)

    RDFFastTreeGraphKernelUtil.predictLogisticRegressionMLLIB(data,2,iteration)

    val t2 = System.nanoTime
    RDFFastTreeGraphKernelUtil.printTime("Run Prediction", t1, t2)

  }


}
