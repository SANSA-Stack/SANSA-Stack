package net.sansa_stack.query.tests

import java.net.URI
import java.nio.file.Paths

import org.apache.jena.query.{Query, QueryExecutionFactory, QueryFactory, ResultSetFormatter}
import org.apache.jena.rdf.model.{Model, ModelFactory, RDFList}
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.jena.util.SplitIRI
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

object SPARQLQueryEvaluationTestSuite {

  def main(args: Array[String]): Unit = {
    new SPARQLQueryEvaluationTestSuite("/sparql11/data-sparql11/manifest-sparql11-query.ttl").tests.foreach(println(_))
  }

  val DEFAULT_QUERY: Query = QueryFactory.create(
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
  |    mf:action
  |         [ qt:query  ?queryFile ;
  |           qt:data   ?dataFile ] ;
  |    mf:result  ?resultsFile .
  |OPTIONAL {?test rdfs:comment ?description }
  |}
  |""".stripMargin)
}

/**
 * @author Lorenz Buehmann
 */
class SPARQLQueryEvaluationTestSuite(manifestPath: String, patternQuery: Query = SPARQLQueryEvaluationTestSuite.DEFAULT_QUERY)
  extends AnyFunSuite {

  // contains the list of ignored tests cases, must be overridden
  lazy val IGNORE: Set[String] = Set.empty[String]

  private def loadTestCasesFromManifest(path: String): List[SPARQLQueryEvaluationTest] = {
    val uri = URI.create(path)
    val url = if (uri.isAbsolute) {
      uri.toURL
    } else {
      classOf[SPARQLQueryEvaluationTestSuite].getResource(path)
    }
    val baseURI = Paths.get(url.toURI).getParent.toString + "/"
    val model = ModelFactory.createDefaultModel()

    RDFDataMgr.read(model, url.getPath, baseURI, Lang.TURTLE)

    // load test cases
    val tests = extractTests(model)

    // process nested manifests
    val it = model.listObjectsOfProperty(model.createProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#include"))
    val includesList = if (it.hasNext) {
      it.next().as(classOf[RDFList]).asJavaList().asScala
    } else {
      Iterator.empty
    }

    tests ++ includesList.flatMap(subDir => loadTestCasesFromManifest(subDir.asResource().getURI)).toList
  }

  private def extractTests(model: Model) = {
    val qe = QueryExecutionFactory.create(patternQuery, model)
    val rs = qe.execSelect()

    ResultSetFormatter.toList(rs).asScala.map(qs => {
      val desc = if (qs.get("description") != null) qs.getLiteral("description").getLexicalForm else ""

      SPARQLQueryEvaluationTest(
        qs.getResource("test").getURI,
        Option(qs.getLiteral("name")).map(_.getLexicalForm).getOrElse(SplitIRI.localname(qs.getResource("test").getURI)),
        desc,
        qs.getResource("queryFile").getURI,
        qs.getResource("dataFile").getURI,
        Option(qs.getResource("resultsFile")).map(_.getURI)
      )
    }
    ).toList
  }


  /**
   * the list of SPARQL evaluation tests
   */
  val tests: List[SPARQLQueryEvaluationTest] = loadTestCasesFromManifest(manifestPath)


}

