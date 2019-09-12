package net.sansa_stack.owl.spark.rdd

import org.apache.spark.sql.SparkSession

import net.sansa_stack.owl.common.parsing.{OWLXMLSyntaxExpressionBuilder, OWLXMLSyntaxParsing}

object OWLXMLSyntaxOWLExpressionsRDDBuilder {


  /**
    * definition to build owl expressions out of OWLXML syntax
    * @param spark: spark session
    * @param filePath: absolute path of the OWL file
    * @return RDDs consisting following expressions
    *         xmlVersion string : <?xml version...?>
    *         owlPrefix string : <rdf:RDF xmlns:.. >
    *         owlExpression string : <owl:Class, ObjectProperty, DatatypeProperty.../owl>
    */

  def build(spark: SparkSession, filePath: String): (OWLExpressionsRDD, OWLExpressionsRDD, OWLExpressionsRDD) = {

    val builder = new OWLXMLSyntaxExpressionBuilder(spark, filePath)

    val owlExpressionsRDD : (OWLExpressionsRDD, OWLExpressionsRDD, OWLExpressionsRDD) = builder.getOwlExpressions()

    owlExpressionsRDD
  }

}
