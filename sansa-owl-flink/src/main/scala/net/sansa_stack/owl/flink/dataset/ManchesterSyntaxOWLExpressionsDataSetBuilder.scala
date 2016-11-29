package net.sansa_stack.owl.flink.dataset

import net.sansa_stack.owl.common.parsing.{ManchesterSyntaxExpressionBuilder, ManchesterSyntaxPrefixParsing}
import net.sansa_stack.owl.flink.hadoop.ManchesterSyntaxInputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.hadoop.io.{LongWritable, Text}


object ManchesterSyntaxOWLExpressionsDataSetBuilder extends  ManchesterSyntaxPrefixParsing {
  def build(env: ExecutionEnvironment, filePath: String): OWLExpressionsDataSet = {
    import org.apache.flink.api.scala._
    val hadoopDataSet: DataSet[(LongWritable, Text)] =
      env.readHadoopFile[LongWritable, Text](
        new ManchesterSyntaxInputFormat,
        classOf[LongWritable],
        classOf[Text],
        filePath
      )
    val rawDataSet = hadoopDataSet.map(_._2.toString)

    val tmp: Seq[(String, String)] = rawDataSet.filter(isPrefixDeclaration(_)).map(parsePrefix(_)).collect()
    val prefixes: Map[String, String] = tmp.toMap

    val builder = new ManchesterSyntaxExpressionBuilder(prefixes)

    rawDataSet.map(builder.clean(_)).filter(_ != null)
  }
}
