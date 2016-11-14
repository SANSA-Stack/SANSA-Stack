package net.sansa_stack.owl.spark.rdd

import com.typesafe.scalalogging.Logger
import net.sansa_stack.owl.common.parsing.FunctionalSyntaxParsing
import org.apache.spark.SparkContext
import org.semanticweb.owlapi.io.OWLParserException


object FunctionalSyntaxOWLAxiomsRDDBuilder extends FunctionalSyntaxParsing {
  private val logger = Logger(this.getClass)

  def build(sc: SparkContext, filePath: String): OWLAxiomsRDD = {
    build(sc, FunctionalSyntaxOWLExpressionsRDDBuilder.build(sc, filePath))
  }

  // FIXME: It has to be ensured that expressionsRDD is in functional syntax
  def build(sc: SparkContext, expressionsRDD: OWLExpressionsRDD): OWLAxiomsRDD = {
    expressionsRDD.map(expression => {
      try makeAxiom(expression)
      catch {
        case exception: OWLParserException => {
          logger.warn("Parser error for line " + expression + ": " + exception.getMessage)
          null
        }
      }
    }).filter(_ != null)
  }
}
