package net.sansa_stack.owl.spark.rdd

import scala.collection.JavaConverters._
import scala.collection.immutable.Set

import com.typesafe.scalalogging.{Logger => ScalaLogger}
import org.apache.log4j.{Level, Logger => Log4JLogger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.io.OWLParserException
import org.semanticweb.owlapi.model.OWLAxiom

import net.sansa_stack.owl.common.OWLSyntax
import net.sansa_stack.owl.common.parsing.OWLXMLSyntaxParsing

object OWLXMLSyntaxOWLAxiomsRDDBuilder extends Serializable with OWLXMLSyntaxParsing {

  private val logger = ScalaLogger(this.getClass)

  /**
    * definition to build OWLAxioms out of OWL file
    * @param spark: spark session
    * @param filePath: absolute path of the OWL file
    * @return OwlAxioms: RDD[Set[OwlAxioms]s]
    * */

  def build(spark: SparkSession, filePath: String): OWLAxiomsRDD = {

    build(spark, OWLXMLSyntaxOWLExpressionsRDDBuilder.build(spark, filePath))
  }

  /**
    * definition to build OwlAxioms out of expressions
    * @param spark: spark session
    * @param owlRecordsRDD: a tuple consisting of RDD records for xmlVersion string, owlxml prefix string and owl expressions
    * */

  def build(spark: SparkSession, owlRecordsRDD: (OWLExpressionsRDD, OWLExpressionsRDD, OWLExpressionsRDD)): OWLAxiomsRDD = {

    // get RDD consisting of xmlVersion
    val xmlVersionRDD = owlRecordsRDD._1.first()

    // get RDD consisting of owlxml prefix
    val owlPrefixRDD = owlRecordsRDD._2.first()

    // get RDD consisting of owl expressions in owlxml syntax
    val owlExpressionsRDD = owlRecordsRDD._3

    // for each owl expressions try to extract axioms in it,
    // if not print the corresponding expression for which axioms could not extracted using owl api

    val expr2Axiom: RDD[Set[OWLAxiom]] = owlExpressionsRDD.map(expressionRDD => {
      try makeAxiom(xmlVersionRDD, owlPrefixRDD, expressionRDD)
      catch {
        case exception: OWLParserException =>
          logger.warn("Parser error for line " + expressionRDD + ": " + exception.getMessage)
          null
      }
    }).filter(_ != null)

    val owlAxiomsRdd = expr2Axiom.flatMap(x => x.toList.asJavaCollection.asScala).distinct()

    val refinedRDD = refineOWLAxioms(spark.sparkContext, owlAxiomsRdd)

    refinedRDD

  }
 }
