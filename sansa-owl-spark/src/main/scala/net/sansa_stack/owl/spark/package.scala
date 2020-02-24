package net.sansa_stack.owl.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.model.{AxiomType, OWLAxiom}

import net.sansa_stack.owl.spark.rdd._

/**
 * Wrap up implicit classes/methods to read OWL data into either
 * [[org.apache.spark.sql.Dataset]] or [[org.apache.spark.rdd.RDD]].
 *
 * @author Gezim Sejdiu
 */

package object owl {

  object Syntax extends Enumeration {
    val FUNCTIONAL, MANCHESTER, OWLXML = Value
  }

  /**
   * Adds methods, `owl(syntax: Syntax)`, `functional` and `manchester`, to
   * [[org.apache.spark.sql.SparkSession]] that allows to read owl files.
   */
  implicit class OWLAxiomReader(spark: SparkSession) {

    /**
     * Load RDF data into a [[org.apache.spark.rdd.RDD]][OWLAxiom]. Currently,
     * only functional, manchester and OWL/XML syntax are supported
     * @param syntax of the OWL (functional, manchester or OWL/XML)
     * @return a [[net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD]]
     */
    def owl(syntax: Syntax.Value): String => OWLAxiomsRDD = syntax match {
      case Syntax.FUNCTIONAL => functional
      case Syntax.MANCHESTER => manchester
      case Syntax.OWLXML => owlXml
      case _ => throw new IllegalArgumentException(
        s"$syntax syntax not supported yet!")
    }

    /**
     * Load OWL data in Functional syntax into an
     * [[org.apache.spark.rdd.RDD]][OWLAxiom].
     * @return the [[net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD]]
     */
    def functional: String => OWLAxiomsRDD = path => {
      FunctionalSyntaxOWLAxiomsRDDBuilder.build(spark, path)
    }

    /**
     * Load OWL data in Manchester syntax into an
     * [[org.apache.spark.rdd.RDD]][OWLAxiom].
     * @return the [[net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD]]
     */
    def manchester: String => OWLAxiomsRDD = path => {
      ManchesterSyntaxOWLAxiomsRDDBuilder.build(spark, path)
    }

    /**
      * Load OWL data in OWL/XML syntax into an
      * [[org.apache.spark.rdd.RDD]][OWLAxiom].
      * @return the [[net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD]]
      */
    def owlXml: String => OWLAxiomsRDD = path => {
      OWLXMLSyntaxOWLAxiomsRDDBuilder.build(spark, path)
    }

  }

  implicit class OWLExpressionsRDDReader(spark: SparkSession) {
    /**
      * Load RDF data into an [[org.apache.spark.rdd.RDD]][String]. Currently,
      * only functional, manchester and OWL/XML syntax are supported
      * @param syntax of the OWL (functional or manchester or OWL/XML)
      * @return a [[net.sansa_stack.owl.spark.rdd.OWLExpressionsRDD]]
      */
     def owlExpressions(syntax: Syntax.Value): String => OWLExpressionsRDD = syntax match {
       case Syntax.FUNCTIONAL => functional
       case Syntax.MANCHESTER => manchester
       case Syntax.OWLXML => owlXml
       case _ => throw new IllegalArgumentException(
         s"$syntax syntax not supported yet!")
     }

     /**
      * Load OWL data in Functional syntax into an
      * [[org.apache.spark.rdd.RDD]][String].
      * @return the [[net.sansa_stack.owl.spark.rdd.OWLExpressionsRDD]]
      */
     def functional: String => OWLExpressionsRDD = path => {
       FunctionalSyntaxOWLExpressionsRDDBuilder.build(spark, path)
     }

     /**
      * Load OWL data in Manchester syntax into an
      * [[org.apache.spark.rdd.RDD]][String].
      * @return the [[net.sansa_stack.owl.spark.rdd.OWLExpressionsRDD]]
      */
     def manchester: String => OWLExpressionsRDD = path => {
       ManchesterSyntaxOWLExpressionsRDDBuilder.build(spark, path)
     }

     /**
       * Load OWL data in OWLXML syntax into an
       * [[org.apache.spark.rdd.RDD]][String].
       * @return the [[net.sansa_stack.owl.spark.rdd.OWLExpressionsRDD]]
       */
     def owlXml: String => OWLExpressionsRDD = path => {
       OWLXMLSyntaxOWLExpressionsRDDBuilder.build(spark, path)._3
     }
  }
}

package object owlAxioms {
  def extractAxioms(axiom: RDD[OWLAxiom], T: AxiomType[_]): RDD[OWLAxiom] =
    axiom.filter(a => a.getAxiomType.equals(T))
}
