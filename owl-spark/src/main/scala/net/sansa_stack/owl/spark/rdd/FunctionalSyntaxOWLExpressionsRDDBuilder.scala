package net.sansa_stack.owl.spark.rdd

import net.sansa_stack.owl.common.parsing.{FunctionalSyntaxExpressionBuilder, FunctionalSyntaxInputFormat, FunctionalSyntaxPrefixParsing}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkContext


object FunctionalSyntaxOWLExpressionsRDDBuilder extends Serializable with FunctionalSyntaxPrefixParsing {
  def build(sc: SparkContext, filePath: String): OWLExpressionsRDD = {
    val hadoopRDD = sc.hadoopFile(
      filePath, classOf[FunctionalSyntaxInputFormat], classOf[LongWritable],
      classOf[Text], sc.defaultMinPartitions)

    val rawRDD = hadoopRDD.map(entry => entry._2.toString)

    val tmp: Array[(String, String)] =
          rawRDD.filter(isPrefixDeclaration(_)).map(parsePrefix).collect()
    val prefixes: Map[String, String] = tmp.toMap

    val builder = new FunctionalSyntaxExpressionBuilder(prefixes)

    rawRDD.map(builder.clean(_)).filter(_ != null)
  }
}
