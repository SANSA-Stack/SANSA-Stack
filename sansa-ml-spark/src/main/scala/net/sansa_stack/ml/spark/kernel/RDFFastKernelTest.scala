package net.sansa_stack.ml.spark.kernel

import java.io.File

import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleRDD
import org.apache.jena.graph
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDFFastKernelTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("fast graph kernel")
      .config("spark.driver.maxResultSize", "3g")
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)


//    tripleNums(sparkSession)

//    doAffiliation(sparkSession)

//    doMultiContract(sparkSession)

//    doTheme(sparkSession, "10")
//    doTheme(sparkSession, "20")
//    doTheme(sparkSession, "40")
//    doTheme(sparkSession, "60")
//    doTheme(sparkSession, "80")
//    doTheme(sparkSession, "")

    doDepthExperiment(sparkSession)

    sparkSession.stop()
  }

  def tripleNums(sparkSession: SparkSession): Unit = {
    val input1 = "src/main/resources/kernel/aifb-fixed_no_schema4.nt"
    val triples1: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input1))

    val input2 = "src/main/resources/kernel/LDMC_Task2_train.nt"
    val triples2: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input2))

    val input31 = "src/main/resources/kernel/Lexicon_NamedRockUnit_t20.nt"
    val triples31: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input31))

    val input32 = "src/main/resources/kernel/Lexicon_NamedRockUnit_t40.nt"
    val triples32: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input32))

    val input33 = "src/main/resources/kernel/Lexicon_NamedRockUnit_t60.nt"
    val triples33: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input33))

    val input34 = "src/main/resources/kernel/Lexicon_NamedRockUnit_t80.nt"
    val triples34: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input34))

    val input35 = "src/main/resources/kernel/Lexicon_NamedRockUnit_t.nt"
    val triples35: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input35))

    println("affiliation: " +  triples1.count())
    println("multi-contract: " +  triples2.count())
    println("theme 20%: " +  triples31.count())
    println("theme 40%: " +  triples32.count())
    println("theme 60%: " +  triples33.count())
    println("theme 70%: " +  triples34.count())
    println("theme 100%: " +  triples35.count())



  }

  def doDepthExperiment(sparkSession: SparkSession): Unit = {
//    println("## Affiliation Prediction")
//    println("# Fast Tree Graph Kernel Depth 1")
//    RDFFastTreeGraphKernelApp.experimentAffiliationPrediction(sparkSession, 1, 1)

//    println("# Fast Tree Graph Kernel Depth 2")
//    RDFFastTreeGraphKernelApp.experimentAffiliationPrediction(sparkSession, 2, 1)

//    println("# Fast Tree Graph Kernel Depth 3")
//    RDFFastTreeGraphKernelApp.experimentAffiliationPrediction(sparkSession, 3, 1)

//    println("# Fast Tree Graph Kernel V2 Depth 1")
//    RDFFastTreeGraphKernelApp_v2.experimentAffiliationPrediction(sparkSession, 1, 1)

//    println("# Fast Tree Graph Kernel V2 Depth 2")
//    RDFFastTreeGraphKernelApp_v2.experimentAffiliationPrediction(sparkSession, 2, 1)

//    println("# Fast Tree Graph Kernel V2 Depth 3")
//    RDFFastTreeGraphKernelApp_v2.experimentAffiliationPrediction(sparkSession, 3, 1)


    println("")
    println("## Theme Prediction 10%")

    RDFFastGraphKernelApp.experimentThemePrediction(sparkSession, 1, "10")
//    println("# Fast Tree Graph Kernel Depth 1")
//    RDFFastTreeGraphKernelApp.experimentThemePrediction(sparkSession, 1, 1, "10")
//
//    println("# Fast Tree Graph Kernel Depth 2")
//    RDFFastTreeGraphKernelApp.experimentThemePrediction(sparkSession, 2, 1, "10")
//
//    println("# Fast Tree Graph Kernel Depth 3")
//    RDFFastTreeGraphKernelApp.experimentThemePrediction(sparkSession, 3, 1, "10")
//
//    println("# Fast Tree Graph Kernel V2 Depth 1")
//    RDFFastTreeGraphKernelApp_v2.experimentThemePrediction(sparkSession, 1, 1, "10")
//
//    println("# Fast Tree Graph Kernel V2 Depth 2")
//    RDFFastTreeGraphKernelApp_v2.experimentThemePrediction(sparkSession, 2, 1, "10")
//
//
//    println("# Fast Tree Graph Kernel V2 Depth 3")
//    RDFFastTreeGraphKernelApp_v2.experimentThemePrediction(sparkSession, 3, 1, "10")
  }

  def doAffiliation(sparkSession: SparkSession): Unit = {
    println("## Affiliation Prediction")

    println("# Fast Graph Kernel")
    RDFFastGraphKernelApp.experimentAffiliationPrediction(sparkSession, 10)

    println("# Fast Tree Graph Kernel Depth 1")
    RDFFastTreeGraphKernelApp.experimentAffiliationPrediction(sparkSession, 1, 10)

    println("# Fast Tree Graph Kernel V2 Depth 1")
    RDFFastTreeGraphKernelApp_v2.experimentAffiliationPrediction(sparkSession, 1, 10)

  }

  def doMultiContract(sparkSession: SparkSession): Unit = {
    println("## Multi-Contract Prediction")

    println("# Fast Graph Kernel Test")
    RDFFastGraphKernelApp.experimentMultiContractPrediction(sparkSession, 10)

    println("# Fast Tree Graph Kernel Depth 1")
    RDFFastTreeGraphKernelApp.experimentMultiContractPrediction(sparkSession, 1, 10)


    println("# Fast Tree Graph Kernel V2 Depth 1")
    RDFFastTreeGraphKernelApp_v2.experimentMultiContractPrediction(sparkSession, 1, 10)
  }

  def doTheme(sparkSession: SparkSession, fraction: String = ""): Unit = {
    println("## Theme Prediction " + fraction + "%")

    println("# Fast Graph Kernel Test")
    RDFFastGraphKernelApp.experimentThemePrediction(sparkSession, 5, fraction)

    println("# Fast Tree Graph Kernel Depth 1")
    RDFFastTreeGraphKernelApp.experimentThemePrediction(sparkSession, 1, 5, fraction)

    println("# Fast Tree Graph Kernel V2 Depth 1")
    RDFFastTreeGraphKernelApp_v2.experimentThemePrediction(sparkSession, 1, 5, fraction)
  }

}
