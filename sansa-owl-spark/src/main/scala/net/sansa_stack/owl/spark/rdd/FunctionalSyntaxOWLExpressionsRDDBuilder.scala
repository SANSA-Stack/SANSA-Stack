package net.sansa_stack.owl.spark.rdd

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.sql.SparkSession

import net.sansa_stack.owl.common.parsing.{FunctionalSyntaxExpressionBuilder, FunctionalSyntaxPrefixParsing}
import net.sansa_stack.owl.spark.hadoop.FunctionalSyntaxInputFormat


object FunctionalSyntaxOWLExpressionsRDDBuilder extends Serializable with FunctionalSyntaxPrefixParsing {
  def build(spark: SparkSession, filePath: String): OWLExpressionsRDD = {
    val hadoopRDD = spark.sparkContext.hadoopFile(
      filePath, classOf[FunctionalSyntaxInputFormat], classOf[LongWritable],
      classOf[Text], spark.sparkContext.defaultMinPartitions)

    val rawRDD = hadoopRDD.map(entry => entry._2.toString)

    val tmp: Array[(String, String)] =
          rawRDD.filter(isPrefixDeclaration(_)).map(parsePrefix).collect()
    val prefixes: Map[String, String] = tmp.toMap

    val builder = new FunctionalSyntaxExpressionBuilder(prefixes)

    rawRDD.map(builder.clean(_)).filter(_ != null)
  }
}
