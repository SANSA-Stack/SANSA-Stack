package net.sansa_stack.owl.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import net.sansa_stack.owl.spark.rdd._

/**
 * Wrap up implicit classes/methods to read OWL data into either [[Dataset]] or
 * [[RDD]].
 *
 * @author Gezim Sejdiu
 */

package object owl {

  object Syntax extends Enumeration {
    val FUNCTIONAL, MANCHESTER, OWLXML = Value
  }

  /**
   * Adds methods, `owl(syntax: Syntax)`, `functional` and `manchester`, to [[SparkSession]] that allows to read owl files.
   */
  implicit class OWLAxiomReader(spark: SparkSession) {

    /**
     * Load RDF data into a `RDD[OWLAxiom]. Currently, only functional and manchester syntax are supported
     * @param syntax of the OWL (functional or manchester)
     * @return a [[OWLAxiomsRDD]]
     */
    def owl(syntax: Syntax.Value): String => OWLAxiomsRDD = syntax match {
      case Syntax.FUNCTIONAL => functional
      case Syntax.MANCHESTER => manchester
      case _ => throw new IllegalArgumentException(s"${syntax} syntax not supported yet!")
    }

    /**
     * Load OWL data in Functional syntax into an [[RDD]][OWLAxiom].
     * @return the [[OWLAxiomsRDD]]
     */
    def functional: String => OWLAxiomsRDD = path => {
      FunctionalSyntaxOWLAxiomsRDDBuilder.build(spark, path)
    }

    /**
     * Load OWL data in Manchester syntax into an [[RDD]][OWLAxiom].
     * @return the [[OWLAxiomsRDD]]
     */
    def manchester: String => OWLAxiomsRDD = path => {
      ManchesterSyntaxOWLAxiomsRDDBuilder.build(spark, path)
    }

  }

  implicit class OWLExpressionsRDDReader(spark: SparkSession) {

  /**
     * Load RDF data into a `RDD[String]. Currently, only functional and manchester syntax are supported
     * @param syntax of the OWL (functional or manchester)
     * @return a [[OWLExpressionsRDD]]
     */
    def owlExpressions(syntax: Syntax.Value): String => OWLExpressionsRDD = syntax match {
      case Syntax.FUNCTIONAL => functional
      case Syntax.MANCHESTER => manchester
      case _ => throw new IllegalArgumentException(s"${syntax} syntax not supported yet!")
    }

    /**
     * Load OWL data in Functional syntax into an [[RDD]][String].
     * @return the [[OWLExpressionsRDD]]
     */
    def functional: String => OWLExpressionsRDD = path => {
      FunctionalSyntaxOWLExpressionsRDDBuilder.build(spark, path)
    }

    /**
     * Load OWL data in Manchester syntax into an [[RDD]][String].
     * @return the [[OWLExpressionsRDD]]
     */
    def manchester: String => OWLExpressionsRDD = path => {
      ManchesterSyntaxOWLExpressionsRDDBuilder.build(spark, path)
    }

  }

}
