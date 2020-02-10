package net.sansa_stack.query.tests

import java.net.URL

import org.apache.jena.query._
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.resultset.rw.ResultsStAX
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.jena.sparql.resultset.{ResultSetCompare, SPARQLResult}
import org.scalatest.FunSuite

import net.sansa_stack.query.tests.util.withResources

/**
 * SPARQL 1.1 test suite runner.
 *
 * Inheriting classes have to implement the method
 * [[net.sansa_stack.query.tests.SPARQLQueryEvaluationTestSuiteRunner#runQuery(org.apache.jena.query.Query, org.apache.jena.rdf.model.Model)]] method.
 *
 * @author Lorenz Buehmann
 */
abstract class SPARQLQueryEvaluationTestSuiteRunner
  extends FunSuite {

  // below vars hold the namespaces for different types of test cases
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

  // contains the list of ignored tests case IDs, must be overridden
  lazy val IGNORE: Set[String] = Set.empty[String]

  // an optional filter function to ignore test cases
  lazy val IGNORE_FILTER: SPARQLQueryEvaluationTest => Boolean = _ => {true}

  // holds the test data
  val testData: List[SPARQLQueryEvaluationTest] = new SPARQLQueryEvaluationTestSuite().tests

  // the main loop over the test data starts here
  // a single ScalaTest is generated per query
  testData
    .filter(data => !IGNORE.contains(data.name))
    .filter(IGNORE_FILTER)
    .slice(0, 50)
    .filter(data => data.name.startsWith("AVG with GROUP BY"))
    .foreach { d =>

      // get the relevant data from the test case
      val queryFileURL = d.queryFile
      val resultFileURL = d.resultsFile
      val datasetURL = d.dataFile
      val testName = d.name

      // test starts here
      test(s"testing $testName") {
        val queryString = readQueryString(queryFileURL)
        val query = QueryFactory.create(queryString)
        println(s"SPARQL query:\n $query")

        // load data
        val data = loadData(datasetURL)
        data.setNsPrefix("", "http://www.example.org/")
        println("Data:")
        data.write(System.out, "Turtle")

        // run the SPARQL query
        val actualResult = runQuery(query, data)

        // read expected result
        val expectedResult = readExpectedResult(resultFileURL)

        // compare results
        if (query.isSelectType) {
          processSelect(query, expectedResult, actualResult)
        } else if (query.isAskType) {
          processAsk(query, expectedResult, actualResult)
        } else {
          fail(s"unsupported query type: ${query.getQueryType}")
        }
      }
    }

  /**
   * Executes the given SPARQL query and returns the result. The result must be a resultset, Boolean value or a
   * model depending on the query type.
   *
   * @param query the SPARQL query
   * @param data the data
   * @return the SPARQL result
   */
  def runQuery(query: Query, data: Model): SPARQLResult

  private def processAsk(query: Query, resultExpected: SPARQLResult, resultActual: SPARQLResult) = {
    assert(resultActual.getBooleanResult == resultExpected.getBooleanResult, "Result of ASK query does not match")
  }

  private def processSelect(query: Query, results: SPARQLResult, resultsAct: SPARQLResult) = {
    val resultsActual = ResultSetFactory.makeRewindable(resultsAct.getResultSet)

    val resultsExpected =
      if (results.isResultSet) {
        ResultSetFactory.makeRewindable(results.getResultSet)
      } else if (results.isModel) {
        ResultSetFactory.makeRewindable(results.getModel)
      } else {
        fail("Wrong result type for SELECT query")
        null
      }

    // compare results
    val b = resultSetEquivalent(query, resultsActual, resultsExpected)

    // print error message
    if (!b) {
      resultsExpected.reset()
      resultsActual.reset()
      println(
        s"""
          |=================================
          |Failure:
          |Query:
          |$query
          |Got: ${resultsActual.size()} ---------------------
          |${ResultSetFormatter.asText(resultsActual, query.getPrologue)}
          |Expected: ${resultsExpected.size()} ------------------
          |${ResultSetFormatter.asText(resultsExpected, query.getPrologue)}
          |""".stripMargin
      )
    }
    assert(b, "Results of SELECT query do not match")

  }

  private def resultSetEquivalent(query: Query, resultsActual: ResultSet, resultsExpected: ResultSet): Boolean = {
    val testByValue = true

    if (testByValue) {
      if (query.isOrdered) {
        ResultSetCompare.equalsByValueAndOrder(resultsExpected, resultsActual)
      } else {
        ResultSetCompare.equalsByValue(resultsExpected, resultsActual)
      }
    } else {
      if (query.isOrdered) {
        ResultSetCompare.equalsByTermAndOrder(resultsExpected, resultsActual)
      } else {
        ResultSetCompare.equalsByTerm(resultsExpected, resultsActual)
      }
    }
  }

  private def readQueryString(queryFileURL: String): String = {
    println(s"loading query from $queryFileURL")
    val is = new URL(queryFileURL).openStream
    withResources[AutoCloseable, String](is)(_ => {
      scala.io.Source.fromInputStream(is).mkString
    })
  }

  private def readExpectedResult(resultFileURL: String): SPARQLResult = {
    println(s"loading expected result from $resultFileURL")
    val is = new URL(resultFileURL).openStream
    ResultsStAX.read(is, ModelFactory.createDefaultModel, null)

    //    ResultSetFactory.load(is, ResultsFormat.FMT_RS_XML)
  }

  private def loadData(datasetURL: String): Model = {
    println(s"loading data from $datasetURL")
    import java.io.IOException
    import java.net.MalformedURLException

    import util._
    try {
      val is = new URL(datasetURL).openStream()

      withResources[AutoCloseable, Model](is)(_ => {
        val data = ModelFactory.createDefaultModel()
        RDFDataMgr.read(data, is, null, if (datasetURL.endsWith(".rdf")) Lang.RDFXML else Lang.TURTLE)
        data
      })

    } catch {
      case e: MalformedURLException =>
        System.err.println("Malformed input URL: " + datasetURL)
        throw e
      case e: IOException =>
        System.err.println("IO error open connection")
        throw e
    }
  }
}
