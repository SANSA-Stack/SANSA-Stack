package net.sansa_stack.ml.spark.kernel

import java.io.File

import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleRDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.SparkContext._



object RDFFastGraphKernelApp {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("fast graph kernel")
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    testSample(sparkSession)
//    experimentAffiliationPrediction(sparkSession)


    sparkSession.stop
  }



  def testSample(sparkSession: SparkSession): Unit = {
//    val input = "sansa-ml-spark/src/main/resources/kernel/sample.nt"
    val input = "sansa-ml-spark/src/main/resources/kernel/aifb-fixed_no_schema.nt"

    val tripleRDD = new TripleRDD(NTripleReader.load(sparkSession, new File(input)))
    val propertyToPredict = "http://swrc.ontoware.org/ontology#affiliation"


    tripleRDD.filterPredicates(_.equals(propertyToPredict)).foreach(println(_))

//    val propertyToPredict = ""

//    val rdfFastGraphKernel = RDFFastGraphKernel(sparkSession, tripleRDD, 4, propertyToPredict)
//    rdfFastGraphKernel.showDataSets()
//    val data = rdfFastGraphKernel.computeLabeledFeatureVectors()

    sparkSession.stop
  }



  def experimentAffiliationPrediction(sparkSession: SparkSession): Unit = {
    //    val input = "sansa-ml-spark/src/main/resources/kernel/aifb-fixed_complete.nt"
    val input = "sansa-ml-spark/src/main/resources/kernel/aifb-fixed_no_schema.nt"

    val tripleRDD = new TripleRDD(NTripleReader.load(sparkSession, new File(input)))
    val filteredRDD = new TripleRDD(tripleRDD.filterPredicates(!_.equals("http://swrc.ontoware.org/ontology#employs")))

    val propertyToPredict = "http://swrc.ontoware.org/ontology#affiliation"

    val rdfFastGraphKernel = RDFFastGraphKernel(sparkSession, filteredRDD, 2, propertyToPredict)
    rdfFastGraphKernel.showDataSets()
    val data = rdfFastGraphKernel.computeLabeledFeatureVectors()




        // Some stuff for SVM:

        // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.09, 0.01, 0.09, 0.01, 0.09, 0.01), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    println("data")
    println(data.count())
//    data.foreach(println(_))
    println("training")
    println(training.count())
//    training.foreach(println(_))
    println("test")
    println(test.count())
//    test.foreach(println(_))

    //val numIterations = 100
    //val model = SVMWithSGD.train(training, numIterations)


    sparkSession.stop
  }

}
