package net.sansa_stack.owl.flink.dataset

import org.apache.flink.api.scala.ExecutionEnvironment

import net.sansa_stack.owl.common.parsing.FunctionalSyntaxParsing

object FunctionalSyntaxOWLAxiomsDataSetBuilder extends FunctionalSyntaxParsing {
  def build(env: ExecutionEnvironment, filePath: String): OWLAxiomsDataSet = {
    val expressionsDataSet = FunctionalSyntaxOWLExpressionsDataSetBuilder.build(env, filePath)
    build(env, expressionsDataSet)
  }

  /**
    * FIXME: Somehow have to ensure that expressionsDataSet is actually in OWL functional syntax
    */
  def build(env: ExecutionEnvironment, expressionsDataSet: OWLExpressionsDataSet): OWLAxiomsDataSet = {
    import org.apache.flink.api.scala._
    expressionsDataSet.map(makeAxiom(_)).filter(_ != null)
  }
}
