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

//    val input = "sansa-ml-spark/src/main/resources/kernel/aifb-fixed_complete.nt"
//    val input = "sansa-ml-spark/src/main/resources/kernel/aifb-fixed_no_schema.nt"

    val input = "sansa-ml-spark/src/main/resources/kernel/sample.nt"
    val tripleRDD = new TripleRDD(NTripleReader.load(sparkSession, new File(input)))


    val rdfFastGraphKernel = RDFFastGraphKernel(sparkSession, tripleRDD, 3)
    rdfFastGraphKernel.compute()


    sparkSession.stop

  }


}
