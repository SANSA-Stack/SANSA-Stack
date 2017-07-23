package net.sansa_stack.ml.spark.kernel

import java.io.File

import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.jena.graph
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDFFastTreeGraphKernelApp_v2 {

  def main(args: Array[String]): Unit = {
    val taskNum: Int = scala.io.StdIn.readLine("Task Number?(1=Affiliation, 2=Lithogenesis, 3=Multi-contract, 4=Theme) ").toInt
    val depth: Int = scala.io.StdIn.readLine("Depth? ").toInt

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("fast graph kernel")
      .config("spark.driver.maxResultSize", "3g")
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)
    

    val t0 = System.nanoTime

    if (taskNum == 1) {
      experimentAffiliationPrediction(sparkSession, depth, 10)
    }

    if (taskNum == 3) {
      experimentMultiContractPrediction(sparkSession, depth, 10)
    }

    if (taskNum == 4) {
      experimentThemePrediction(sparkSession, depth, 10)
    }

    RDFFastTreeGraphKernelUtil.printTime("Total: ", t0, System.nanoTime)
    println("taskNum: " + taskNum)
    println("depth: " + depth)
    sparkSession.stop
  }




  def experimentAffiliationPrediction(sparkSession: SparkSession, depth: Int, iteration: Int): Unit = {
    //val input = "src/main/resources/kernel/aifb-fixed_complete4.nt"
    val input = "src/main/resources/kernel/aifb-fixed_no_schema4.nt"

    val t0 = System.nanoTime

    val triples: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input))
    val filteredTriples = triples
      .filter(_.getPredicate.getURI != "http://swrc.ontoware.org/ontology#affiliation")
      .filter(_.getPredicate.getURI != "http://swrc.ontoware.org/ontology#employs")
    val excludedTriples = triples
      .filter(_.getPredicate.getURI == "http://swrc.ontoware.org/ontology#affiliation")

    val tripleDF = RDFFastTreeGraphKernelUtil.triplesToDF(sparkSession, filteredTriples)
    val tripleForInstanceDF = RDFFastTreeGraphKernelUtil.triplesToDF(sparkSession, excludedTriples)
    val instanceDF = RDFFastTreeGraphKernelUtil.getInstanceAndLabelDF(tripleForInstanceDF)
    instanceDF.groupBy("label").count().show()
    //    +-----+-----+
    //    |label|count|
    //    +-----+-----+
    //    |  0.0|   60|
    //    |  1.0|   73|
    //    |  2.0|   28|
    //    |  3.0|   16|
    //    +-----+-----+

    val rdfFastTreeGraphKernel = RDFFastTreeGraphKernel_v2(sparkSession, tripleDF, instanceDF, depth)
    val data = rdfFastTreeGraphKernel.getMLLibLabeledPoints

    val t1 = System.nanoTime

    RDFFastTreeGraphKernelUtil.printTime("Initialization", t0, t1)

    RDFFastTreeGraphKernelUtil.predictLogisticRegressionMLLIB(data,4,iteration)

    val t2 = System.nanoTime


    RDFFastTreeGraphKernelUtil.printTime("Run Prediction", t1, t2)


  }


  def experimentMultiContractPrediction(sparkSession: SparkSession, depth: Int, iteration: Int): Unit = {
    val input = "src/main/resources/kernel/LDMC_Task2_train.nt"

    val t0 = System.nanoTime

    val triples: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input))
    val filteredTriples = triples
      .filter(_.getPredicate.getURI != "http://example.com/multicontract")
    val excludedTriples = triples
      .filter(_.getPredicate.getURI == "http://example.com/multicontract")


    val tripleDF = RDFFastTreeGraphKernelUtil.triplesToDF(sparkSession, filteredTriples)
    val tripleForInstanceDF = RDFFastTreeGraphKernelUtil.triplesToDF(sparkSession, excludedTriples)
    val instanceDF = RDFFastTreeGraphKernelUtil.getInstanceAndLabelDF(tripleForInstanceDF)
    instanceDF.groupBy("label").count().show()
    // +-----+-----+
    // |label|count|
    // +-----+-----+
    // |  0.0|   40|
    // |  1.0|  168|
    // +-----+-----+


    val rdfFastTreeGraphKernel = RDFFastTreeGraphKernel_v2(sparkSession, tripleDF, instanceDF, depth)
    val data = rdfFastTreeGraphKernel.getMLLibLabeledPoints

    val t1 = System.nanoTime

    RDFFastTreeGraphKernelUtil.printTime("Initialization", t0, t1)

    RDFFastTreeGraphKernelUtil.predictLogisticRegressionMLLIB(data,2,iteration)

    val t2 = System.nanoTime


    RDFFastTreeGraphKernelUtil.printTime("Run Prediction", t1, t2)




  }


  def experimentThemePrediction(sparkSession: SparkSession, depth: Int, iteration: Int, fraction: String = ""): Unit = {
    val input = "src/main/resources/kernel/Lexicon_NamedRockUnit_t" + fraction + ".nt"

    val t0 = System.nanoTime

    val triples: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input))
    val filteredTriples = triples.filter(_.getPredicate.getURI != "http://data.bgs.ac.uk/ref/Lexicon/hasTheme")
    val excludedTriples = triples.filter(_.getPredicate.getURI == "http://data.bgs.ac.uk/ref/Lexicon/hasTheme")

    val tripleDF = RDFFastTreeGraphKernelUtil.triplesToDF(sparkSession, filteredTriples)
    val tripleForInstanceDF = RDFFastTreeGraphKernelUtil.triplesToDF(sparkSession, excludedTriples)
    val instanceDF = RDFFastTreeGraphKernelUtil.getInstanceAndLabelDF(tripleForInstanceDF)
//    instanceDF.groupBy("label").count().show()
    //    +-----+-----+
    //    |label|count|
    //    +-----+-----+
    //    |  0.0| 1005|
    //    |  1.0|  137|
    //    +-----+-----+

    val rdfFastTreeGraphKernel = RDFFastTreeGraphKernel_v2(sparkSession, tripleDF, instanceDF, depth)
    val data = rdfFastTreeGraphKernel.getMLLibLabeledPoints

    val t1 = System.nanoTime

    RDFFastTreeGraphKernelUtil.printTime("Initialization", t0, t1)

    RDFFastTreeGraphKernelUtil.predictLogisticRegressionMLLIB(data,2,iteration)

    val t2 = System.nanoTime


    RDFFastTreeGraphKernelUtil.printTime("Run Prediction", t1, t2)



  }







}