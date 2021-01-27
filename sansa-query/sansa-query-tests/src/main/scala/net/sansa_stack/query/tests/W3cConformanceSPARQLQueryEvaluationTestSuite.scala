package net.sansa_stack.query.tests

import scala.collection.JavaConverters._

import org.apache.jena.query.{QueryExecutionFactory, QueryFactory, ResultSetFormatter}
import org.apache.jena.rdf.model.{ModelFactory, RDFList}
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.scalatest.FunSuite


/**
 * @author Lorenz Buehmann
 */
class W3cConformanceSPARQLQueryEvaluationTestSuite(val sparqlVersion: SPARQL_VERSION.Value)
  extends FunSuite {

  protected val aggregatesManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#"
  protected val bindManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/bind/manifest#"
  protected val bindingsManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/bindings/manifest#"
  protected val functionsManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/functions/manifest#"
  protected val constructManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/construct/manifest#"
  protected val csvTscResManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/csv-tsv-res/manifest#"
  protected val groupingManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/grouping/manifest#"
  protected val negationManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/negation/manifest#"
  protected val existsManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/exists/manifest#"
  protected val projectExpressionManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/project-expression/manifest#"
  protected val propertyPathManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/property-path/manifest#"
  protected val subqueryManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/subquery/manifest#"
  protected val serviceManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service/manifest#"

  // contains the list of ignored tests cases, must be overridden
  lazy val IGNORE: Set[String] = Set.empty[String]

  val baseDir = "/sparql11"
  val testDirSPARQL11: String = baseDir + (if (sparqlVersion == SPARQL_VERSION.SPARQL_11) "/data-sparql11/" else "/data-r2/")

  private def loadTestCasesFromManifest(): List[SPARQLQueryEvaluationTest] = {
    val baseURL = classOf[W3cConformanceSPARQLQueryEvaluationTestSuite].getResource(testDirSPARQL11)
    val url = classOf[W3cConformanceSPARQLQueryEvaluationTestSuite].getResource(testDirSPARQL11 + "manifest-sparql11-query.ttl")
    val model = ModelFactory.createDefaultModel()

    RDFDataMgr.read(model, url.getPath, baseURL.getPath, Lang.TURTLE)

    val includesList = model.listObjectsOfProperty(model.createProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#include")).next().as(classOf[RDFList])

    includesList.asJavaList().asScala.flatMap(subDir => loadTestCasesFromSubManifest(subDir.asResource().getURI)).toList
  }

  private def loadTestCasesFromSubManifest(path: String) = {
    val model = RDFDataMgr.loadModel(path, Lang.TURTLE)

    val members = model.listObjectsOfProperty(model.createProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#entries")).next().as(classOf[RDFList])

    val query = QueryFactory.create(
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
        |    mf:name    ?name ;
        |    dawgt:approval dawgt:Approved ;
        |    mf:action
        |         [ qt:query  ?queryFile ;
        |           qt:data   ?dataFile ] ;
        |    mf:result  ?resultsFile .
        |OPTIONAL {?test rdfs:comment ?description }
        |}
        |""".stripMargin)

    val qe = QueryExecutionFactory.create(query, model)
    val rs = qe.execSelect()

    ResultSetFormatter.toList(rs).asScala.map(qs => {
      val desc = if (qs.get("description") != null) qs.getLiteral("description").getLexicalForm else ""

      SPARQLQueryEvaluationTest(
        qs.getResource("test").getURI,
        qs.getLiteral("name").getLexicalForm,
        desc,
        qs.getResource("queryFile").getURI,
        qs.getResource("dataFile").getURI,
        Some(qs.getResource("resultsFile").getURI)
      )
    }
    ).toList
  }


  /**
   * the list of SPARQL evaluation tests
   */
  val tests: List[SPARQLQueryEvaluationTest] = loadTestCasesFromManifest()


}


