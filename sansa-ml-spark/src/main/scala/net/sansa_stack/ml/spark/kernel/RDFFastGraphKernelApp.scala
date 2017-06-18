package net.sansa_stack.ml.spark.kernel

import java.io.File

import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleRDD
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SparkSession


object RDFFastGraphKernelApp {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("fast graph kernel")
      .getOrCreate()

//    val input = "sansa-ml-spark/src/main/resources/kernel/aifb-fixed_complete.nt"
    val input = "sansa-ml-spark/src/main/resources/kernel/sample.nt"
    val tripleRDD = new TripleRDD(NTripleReader.load(sparkSession, new File(input)))
    val instances = tripleRDD.getSubjects.distinct()
//    instances.map(f => f).foreach(println(_))
//    tripleRDD.getTriples.foreach(println(_))

    val featureSet = RDFFastGraphKernel(tripleRDD, instances, 2 ).kernelCompute()
//
    featureSet.foreach(println(_))


//    val tokenizer = new Tokenizer().setInputCol("instance").setOutputCol("path")
//    val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")
//    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01)
//    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))
//
//    triplesRDD.take(5).foreach(println(_))
    sparkSession.stop

  }


}
