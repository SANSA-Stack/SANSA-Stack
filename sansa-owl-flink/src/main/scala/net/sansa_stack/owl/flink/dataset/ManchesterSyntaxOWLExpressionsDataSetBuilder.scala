package net.sansa_stack.owl.flink.dataset

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.hadoopcompatibility.scala.HadoopInputs
import org.apache.hadoop.io.{LongWritable, Text}

import net.sansa_stack.owl.common.parsing.{ManchesterSyntaxExpressionBuilder, ManchesterSyntaxPrefixParsing}
import net.sansa_stack.owl.flink.hadoop.ManchesterSyntaxInputFormat


object ManchesterSyntaxOWLExpressionsDataSetBuilder extends  ManchesterSyntaxPrefixParsing {
  def build(env: ExecutionEnvironment, filePath: String): OWLExpressionsDataSet = {
    buildAndGetPrefixes(env, filePath)._1
  }

  private[dataset] def buildAndGetPrefixes(env: ExecutionEnvironment,
      filePath: String): (OWLExpressionsDataSet, Map[String, String]) = {

    import org.apache.flink.api.scala._
    val wrappedFormat = HadoopInputs.readHadoopFile(new ManchesterSyntaxInputFormat,
      classOf[LongWritable],
      classOf[Text],
      filePath)

    val hadoopDataSet: DataSet[(LongWritable, Text)] = env.createInput(wrappedFormat)

    val rawDataSet = hadoopDataSet.map(_._2.toString)

    val tmp: Seq[(String, String)] = rawDataSet.filter(isPrefixDeclaration(_)).map(parsePrefix(_)).collect()
    val prefixes: Map[String, String] = tmp.toMap

    val builder = new ManchesterSyntaxExpressionBuilder(prefixes)

    (rawDataSet.map(builder.clean(_)).filter(_ != null), prefixes)
  }
}
