package net.sansa_stack.owl.flink

import org.apache.flink.api.scala.{ DataSet, ExecutionEnvironment }

import net.sansa_stack.owl.flink.dataset.{
  FunctionalSyntaxOWLAxiomsDataSetBuilder,
  FunctionalSyntaxOWLExpressionsDataSetBuilder,
  ManchesterSyntaxOWLAxiomsDataSetBuilder,
  ManchesterSyntaxOWLExpressionsDataSetBuilder,
  OWLAxiomsDataSet,
  OWLExpressionsDataSet
}

/**
 * Wrap up implicit classes/methods to read OWL data into a
 * [[org.apache.flink.api.scala.DataSet]].
 *
 * @author Gezim Sejdiu
 */
package object owl {

  object Syntax extends Enumeration {
    val FUNCTIONAL, MANCHESTER = Value
  }

  /**
   * Adds methods, `owl(syntax: Syntax)`, `functional` and `manchester`,
   * to [[org.apache.flink.api.scala.ExecutionEnvironment]] that allows to read
   * owl files.
   */
  implicit class OWLAxiomReader(env: ExecutionEnvironment) {
    /**
     * Load RDF data into an [[org.apache.flink.api.scala.DataSet]][OWLAxiom].
     * Currently, only functional and manchester syntax are supported
     * @param syntax of the OWL (functional or manchester)
     * @return a [[net.sansa_stack.owl.flink.dataset.OWLAxiomsDataSet]]
     */
    def owl(syntax: Syntax.Value): String => OWLAxiomsDataSet = syntax match {
      case Syntax.FUNCTIONAL => functional
      case Syntax.MANCHESTER => manchester
      case _ => throw new IllegalArgumentException(s"${syntax} syntax not supported yet!")
    }

    /**
     * Load OWL data in Functional syntax into an
     * [[org.apache.flink.api.scala.DataSet]][OWLAxiom].
     * @return the [[net.sansa_stack.owl.flink.dataset.OWLAxiomsDataSet]]
     */
    def functional: String => OWLAxiomsDataSet = path => {
      FunctionalSyntaxOWLAxiomsDataSetBuilder.build(env, path)
    }

    /**
     * Load OWL data in Manchester syntax into an
     * [[org.apache.flink.api.scala.DataSet]][OWLAxiom].
     * @return the [[net.sansa_stack.owl.flink.dataset.OWLAxiomsDataSet]]
     */
    def manchester: String => OWLAxiomsDataSet = path => {
      ManchesterSyntaxOWLAxiomsDataSetBuilder.build(env, path)
    }
  }

  implicit class OWLExpressionsReader(env: ExecutionEnvironment) {
    /**
     * Load RDF data into a [[org.apache.flink.api.scala.DataSet]][String].
     * Currently, only functional and manchester syntax are supported
     * @param syntax of the OWL (functional or manchester)
     * @return a [[net.sansa_stack.owl.flink.dataset.OWLExpressionsDataSet]]
     */
    def owlExpressions(syntax: Syntax.Value): String => OWLExpressionsDataSet = syntax match {
      case Syntax.FUNCTIONAL => functional
      case Syntax.MANCHESTER => manchester
      case _ => throw new IllegalArgumentException(
        s"$syntax syntax not supported yet!")
    }

    /**
     * Load OWL data in Functional syntax into an
     * [[org.apache.flink.api.scala.DataSet]][String].
     * @return the [[net.sansa_stack.owl.flink.dataset.OWLExpressionsDataSet]]
     */
    def functional: String => OWLExpressionsDataSet = path => {
      FunctionalSyntaxOWLExpressionsDataSetBuilder.build(env, path)
    }

    /**
     * Load OWL data in Manchester syntax into an
     * [[org.apache.flink.api.scala.DataSet]][String].
     * @return the [[net.sansa_stack.owl.flink.dataset.OWLExpressionsDataSet]]
     */
    def manchester: String => OWLExpressionsDataSet = path => {
      ManchesterSyntaxOWLExpressionsDataSetBuilder.build(env, path)
    }
  }
}
