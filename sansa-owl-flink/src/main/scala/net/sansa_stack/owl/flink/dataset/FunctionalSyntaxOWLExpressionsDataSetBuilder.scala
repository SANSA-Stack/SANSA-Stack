package net.sansa_stack.owl.flink.dataset

import net.sansa_stack.owl.common.parsing.{FunctionalSyntaxExpressionBuilder, FunctionalSyntaxPrefixParsing}
import net.sansa_stack.owl.flink.hadoop.FunctionalSyntaxInputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.hadoopcompatibility.scala.HadoopInputs
import org.apache.hadoop.io.{LongWritable, Text}


object FunctionalSyntaxOWLExpressionsDataSetBuilder extends FunctionalSyntaxPrefixParsing {
  def build(env: ExecutionEnvironment, filePath: String): OWLExpressionsDataSet = {
    import org.apache.flink.api.scala._

    val wrappedFormat = HadoopInputs.readHadoopFile(new FunctionalSyntaxInputFormat,
      classOf[LongWritable],
      classOf[Text],
      filePath)

    val hadoopDataSet: DataSet[(LongWritable, Text)] = env.createInput(wrappedFormat)

    val rawDataSet = hadoopDataSet.map(_._2.toString)

    val tmp: Seq[(String, String)] = rawDataSet.filter(isPrefixDeclaration(_)).map(parsePrefix(_)).collect()
    val prefixes: Map[String, String] = tmp.toMap

    val builder = new FunctionalSyntaxExpressionBuilder(prefixes)

    rawDataSet.map(builder.clean(_)).filter(_ != null)
  }

}
