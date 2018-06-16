package net.sansa_stack.owl.flink

import org.apache.flink.api.scala.{ DataSet, ExecutionEnvironment }

import net.sansa_stack.owl.flink.dataset.{
  OWLAxiomsDataSet,
  OWLExpressionsDataSet,
  FunctionalSyntaxOWLAxiomsDataSetBuilder,
  ManchesterSyntaxOWLAxiomsDataSetBuilder,
  FunctionalSyntaxOWLExpressionsDataSetBuilder,
  ManchesterSyntaxOWLExpressionsDataSetBuilder
}

/**
 * Wrap up implicit classes/methods to read OWL data into a [[DataSet]].
 *
 * @author Gezim Sejdiu
 */
package object owl {

  object Syntax extends Enumeration {
    val FUNCTIONAL, MANCHESTER, OWLXML = Value
  }

  /**
   * Adds methods, `owl(syntax: Syntax)`, `functional` and `manchester`,
   * to [[ExecutionEnvironment]] that allows to read owl files.
   */
  implicit class OWLAxiomReader(env: ExecutionEnvironment) {
    /**
     * Load RDF data into a `DataSet[OWLAxiom].
     * Currently, only functional and manchester syntax are supported
     * @param syntax of the OWL (functional or manchester)
     * @return a [[OWLAxiomsDataSet]]
     */
    def owl(syntax: Syntax.Value): String => OWLAxiomsDataSet = syntax match {
      case Syntax.FUNCTIONAL => functional
      case Syntax.MANCHESTER => manchester
      case _                 => throw new IllegalArgumentException(s"${syntax} syntax not supported yet!")
    }

    /**
     * Load OWL data in Functional syntax into an [[DataSet]][OWLAxiom].
     * @return the [[OWLAxiomsDataSet]]
     */
    def functional: String => OWLAxiomsDataSet = path => {
      FunctionalSyntaxOWLAxiomsDataSetBuilder.build(env, path)
    }

    /**
     * Load OWL data in Manchester syntax into an [[DataSet]][OWLAxiom].
     * @return the [[OWLAxiomsDataSet]]
     */
    def manchester: String => OWLAxiomsDataSet = path => {
      ManchesterSyntaxOWLAxiomsDataSetBuilder.build(env, path)
    }

  }

  implicit class OWLExpressionsReader(env: ExecutionEnvironment) {
    /**
     * Load RDF data into a `DataSet[String].
     * Currently, only functional and manchester syntax are supported
     * @param syntax of the OWL (functional or manchester)
     * @return a [[OWLExpressionsDataSet]]
     */
    def owlExpressions(syntax: Syntax.Value): String => OWLExpressionsDataSet = syntax match {
      case Syntax.FUNCTIONAL => functional
      case Syntax.MANCHESTER => manchester
      case _                 => throw new IllegalArgumentException(s"${syntax} syntax not supported yet!")
    }

    /**
     * Load OWL data in Functional syntax into an [[DataSet]][String].
     * @return the [[OWLExpressionsDataSet]]
     */
    def functional: String => OWLExpressionsDataSet = path => {
      FunctionalSyntaxOWLExpressionsDataSetBuilder.build(env, path)
    }

    /**
     * Load OWL data in Manchester syntax into an [[DataSet]][String].
     * @return the [[OWLExpressionsDataSet]]
     */
    def manchester: String => OWLExpressionsDataSet = path => {
      ManchesterSyntaxOWLExpressionsDataSetBuilder.build(env, path)
    }

  }

}