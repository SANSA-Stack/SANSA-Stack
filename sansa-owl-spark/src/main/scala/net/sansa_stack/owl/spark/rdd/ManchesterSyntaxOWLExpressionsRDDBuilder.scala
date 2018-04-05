package net.sansa_stack.owl.spark.rdd

import net.sansa_stack.owl.common.parsing.{ ManchesterSyntaxExpressionBuilder, ManchesterSyntaxPrefixParsing }
import net.sansa_stack.owl.spark.hadoop.ManchesterSyntaxInputFormat
import org.apache.hadoop.io.{ LongWritable, Text }
import org.apache.spark.sql.SparkSession

object ManchesterSyntaxOWLExpressionsRDDBuilder extends ManchesterSyntaxPrefixParsing {
  def build(spark: SparkSession, filePath: String): OWLExpressionsRDD = {
    buildAndGetPrefixes(spark, filePath)._1
  }

  private[spark] def buildAndGetPrefixes(spark: SparkSession, filePath: String): (OWLExpressionsRDD, Map[String, String]) = {
    val rawRDD = spark.sparkContext.hadoopFile(
      filePath,
      classOf[ManchesterSyntaxInputFormat],
      classOf[LongWritable],
      classOf[Text],
      spark.sparkContext.defaultMinPartitions).map(_._2.toString)

    val tmp: Array[(String, String)] =
      rawRDD.filter(isPrefixDeclaration(_)).map(parsePrefix).collect()
    val prefixes: Map[String, String] = tmp.toMap

    val builder = new ManchesterSyntaxExpressionBuilder(prefixes)
    (rawRDD.map(builder.clean(_)).filter(_ != null), prefixes)
  }
}
