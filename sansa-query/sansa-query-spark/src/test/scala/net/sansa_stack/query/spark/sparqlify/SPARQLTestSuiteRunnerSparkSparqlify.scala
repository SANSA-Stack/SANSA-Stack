package net.sansa_stack.query.spark.sparqlify

import net.sansa_stack.query.spark.SPARQLTestSuiteRunnerSpark
import net.sansa_stack.query.spark.api.domain.QueryEngineFactory
import net.sansa_stack.query.tests.SPARQLQueryEvaluationTestSuite
import org.apache.jena.query.{Query, QueryFactory}

object SPARQLTestSuiteRunnerSparkSparqlify {

  val CUSTOM_TS_QUERY: Query = QueryFactory.create(
  """
    |prefix rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    |prefix : <http://www.w3.org/2009/sparql/docs/tests/data-sparql11/construct/manifest#>
    |prefix rdfs:	<http://www.w3.org/2000/01/rdf-schema#>
    |prefix mf:     <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#>
    |prefix qt:     <http://www.w3.org/2001/sw/DataAccess/tests/test-query#>
    |prefix dawgt:   <http://www.w3.org/2001/sw/DataAccess/tests/test-dawg#>
    |
    |SELECT * {
    |?test rdf:type mf:QueryEvaluationTest ;
    |    mf:action
    |         [ qt:query  ?queryFile ;
    |           qt:data   ?dataFile ] .
    |   OPTIONAL { ?test mf:name    ?name }
    |   OPTIONAL { ?test mf:result  ?resultsFile }
    |   OPTIONAL { ?test rdfs:comment ?description }
    |}
    |""".stripMargin)
}

class SPARQLTestSuiteRunnerSparkSparqlify
  extends SPARQLTestSuiteRunnerSpark(new SPARQLQueryEvaluationTestSuite(
    "/sansa-sparql-ts/manifest.ttl",
    SPARQLTestSuiteRunnerSparkSparqlify.CUSTOM_TS_QUERY)) {

  override def getEngineFactory: QueryEngineFactory = new QueryEngineFactorySparqlify(spark)

//  override lazy val IGNORE_FILTER: SPARQLQueryEvaluationTest => Boolean
//  = _.name.contains("15-q1")
}