package net.sansa_stack.owl.spark.rdd

import com.typesafe.scalalogging.Logger
import net.sansa_stack.owl.common.parsing.FunctionalSyntaxParsing
import org.semanticweb.owlapi.io.OWLParserException
import org.apache.spark.sql.SparkSession


object FunctionalSyntaxOWLAxiomsRDDBuilder extends FunctionalSyntaxParsing {
  private val logger = Logger(this.getClass)

  def build(spark: SparkSession, filePath: String): OWLAxiomsRDD = {
    build(spark, FunctionalSyntaxOWLExpressionsRDDBuilder.build(spark, filePath))
  }

  // FIXME: It has to be ensured that expressionsRDD is in functional syntax
  def build(spark: SparkSession, expressionsRDD: OWLExpressionsRDD): OWLAxiomsRDD = {
    expressionsRDD.map(expression => {
      try makeAxiom(expression)
      catch {
        case exception: OWLParserException =>
          logger.warn("Parser error for line " + expression + ": " + exception.getMessage)
          null
      }
    }).filter(_ != null)
  }
}
