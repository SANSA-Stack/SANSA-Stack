package net.sansa_stack.query.tests

import java.io.File
import java.net.{URI, URL}
import java.nio.file.{Path, Paths}
import scala.collection.JavaConverters._
import org.apache.jena.riot.RDFParserBuilder
import org.apache.jena.ext.com.google.common.reflect.ClassPath
import org.apache.jena.iri.{IRIFactory, ViolationCodes}
import org.apache.jena.irix.IRIxResolver
import org.apache.jena.query.{QueryExecutionFactory, QueryFactory, ResultSetFormatter}
import org.apache.jena.rdf.model.{ModelFactory, RDFList, Resource}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFParserBuilder}
import org.apache.jena.util.FileManager
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

  private def resolveGlobal(s: String) = {
    val globalBase = "file://" + new File("").toURI.toString.substring(5)
    val factory = new IRIFactory(IRIFactory.jenaImplementation)

    factory.shouldViolation(false, false)
    factory.securityViolation(false, false)
    factory.setIsWarning(ViolationCodes.UNREGISTERED_IANA_SCHEME, false)
    factory.setIsError(ViolationCodes.UNREGISTERED_IANA_SCHEME, false)
    factory.setSameSchemeRelativeReferences("file")

    val cwd = factory.construct(globalBase)

    cwd.resolve(s).toString
  }

  private def loadTestCasesFromManifest(): List[SPARQLQueryEvaluationTest] = {
    val baseURL = classOf[W3cConformanceSPARQLQueryEvaluationTestSuite].getResource(testDirSPARQL11)
    val model = ModelFactory.createDefaultModel()

    RDFParserBuilder.create
      .source("sparql11/data-sparql11/manifest-sparql11-query.ttl")
      .checking(false)
      .resolver(IRIxResolver.create().noBase().allowRelative(true).build())
      .parse(model)

    val includesList = model.listObjectsOfProperty(model.createProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#include")).next().as(classOf[RDFList])

    includesList.asJavaList().asScala.flatMap(subDir => loadTestCasesFromSubManifest(subDir.asResource(), "sparql11/data-sparql11/")).toList
  }

  private def loadTestCasesFromSubManifest(r: Resource, base: String) = {
    val model = ModelFactory.createDefaultModel()
    RDFParserBuilder.create
      .source(base + r.getURI)
      .checking(false)
      .resolver(IRIxResolver.create().noBase().allowRelative(true).build())
      .parse(model)

    val uri = URI.create(r.getURI)
    val parent = Paths.get(base + (if (uri.getPath().endsWith("/")) uri.resolve("..") else uri.resolve(".")).toString)

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
        relativePath(qs.getResource("queryFile"), parent),
        relativePath(qs.getResource("dataFile"), parent),
        Some(relativePath(qs.getResource("resultsFile"), parent))
      )
    }
    ).toList
  }

  private def relativePath(r: Resource, path: Path): String = {
    path.resolve(r.getURI).toString
  }


  /**
   * the list of SPARQL evaluation tests
   */
  val tests: List[SPARQLQueryEvaluationTest] = loadTestCasesFromManifest()


}

object W3cConformanceSPARQLQueryEvaluationTestSuite {
  def main(args: Array[String]): Unit = {
    new W3cConformanceSPARQLQueryEvaluationTestSuite(SPARQL_VERSION.SPARQL_11).tests.foreach(println)
  }
}


