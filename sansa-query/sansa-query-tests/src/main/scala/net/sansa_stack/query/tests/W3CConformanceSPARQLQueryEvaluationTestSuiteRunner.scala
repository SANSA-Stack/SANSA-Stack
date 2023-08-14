package net.sansa_stack.query.tests

import net.sansa_stack.query.tests.util._
import org.apache.jena.query._
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.system.stream.StreamManager
import org.apache.jena.riot.{Lang, RDFDataMgr, ReadAnything}
import org.apache.jena.sparql.resultset.{ResultSetCompare, ResultsFormat, SPARQLResult}
import org.scalatest.FunSuite
import scala.collection.mutable


/**
 * SPARQL test suite runner.
 *
 * Inheriting classes have to implement the method
 * [[W3CConformanceSPARQLQueryEvaluationTestSuiteRunner#runQuery(org.apache.jena.query.Query, org.apache.jena.rdf.model.Model)]] method.
 *
 * @param sparqlVersion the SPARQL version of the test suite, i.e. either SPARQL 1.0 or 1.1
 * @author Lorenz Buehmann
 */
abstract class W3CConformanceSPARQLQueryEvaluationTestSuiteRunner(val sparqlVersion: SPARQL_VERSION.Value = SPARQL_VERSION.SPARQL_11)
  extends FunSuite {

  // below vars hold the namespaces for different types of test cases
  // SPARQL 1.0
  protected val algebraManifest: String = "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#"
  protected val basicManifest: String = "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/basic/manifest#"
  protected val booleanManifest: String = "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/boolean-effective-value/manifest#"
  protected val castManifest: String = "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/cast/manifest#"
  protected val constructManifest10: String = "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/construct/manifest#"
  protected val datasetManifest: String = "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/dataset/manifest#"
  protected val distinctManifest: String = "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/distinct/manifest#"
  protected val exprBuiltInManifest: String = "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-builtin/manifest#"
  protected val exprEqualsManifest: String = "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/expr-equals/manifest#"
  protected val graphManifest: String = "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/graph/manifest#"
  protected val openWorldManifest: String = "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#"
  protected val regexManifest: String = "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/regex/manifest#"
  protected val solutionSeqManifest: String = "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/solution-seq/manifest#"
  protected val sortManifest: String = "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/sort/manifest#"
  protected val typePromotionManifest: String = "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/type-promotion/manifest#"
  protected val optionalManifest: String = "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional/manifest#"

  // SPARQL 1.1
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
  lazy val IGNORED_URIS: Set[String] = Set.empty[String]

  // an optional filter function to ignore test cases by name
  lazy val IGNORED_NAMES: mutable.Set[String] = mutable.Set[String]()

  // an optional filter function to ignore test cases
  lazy val FILTER_KEEP: SPARQLQueryEvaluationTest => Boolean = _ => {true}

  // holds the test data
  val testData: List[SPARQLQueryEvaluationTest] = new W3cConformanceSPARQLQueryEvaluationTestSuite(sparqlVersion).tests

  private def isIgnored(testCase: SPARQLQueryEvaluationTest): Boolean = {
    IGNORED_URIS.contains(testCase.uri) || IGNORED_NAMES.exists(testCase.name.startsWith) || !FILTER_KEEP(testCase)
  }

  // the main loop over the test data starts here
  // a single ScalaTest is generated per query
  // we group by data first, to load it just once and perform all tests based on the same data
  testData
    //    .filterNot(isIgnored)
    .groupBy(_.dataFile)
    .foreach { case (dataFile, tests) =>
      // load data
      // (we only have to load the data if there is at least one unignored test)
      if (tests.exists(t => !isIgnored(t))) {

        tests.foreach(testCase => {
          // get the relevant data from the test case
          val queryFileURL = testCase.queryFile
          val resultFileURL = testCase.resultsFile
          val testName = testCase.name
          val description = Option(testCase.description).getOrElse("")

          if (isIgnored(testCase)) {
            ignore(s"$testName: $description") {}
          } else {
            // test starts here
            test(s"$testName: $description") {
              val data = loadData(dataFile)
              runTest(testCase, data)
            }
          }
        })
      }
    }

  def runTest(testCase: SPARQLQueryEvaluationTest, data: Model): Unit = {
    if (data.isEmpty) cancel("cannot handle empty data model - please add test to ignored tests")
    // get the relevant data from the test case
    val queryFileURL = testCase.queryFile
    val resultFileURL = testCase.resultsFile
    val testName = testCase.name

    val queryString = readQueryString(queryFileURL)
    val query = QueryFactory.create(queryString)
    println(s"SPARQL query:\n $query")

    // run the SPARQL query
    val actualResult = runQuery(query, data)

    // read expected result
    val expectedResult = readExpectedResult(resultFileURL.get)

    // compare results
    if (query.isSelectType) {
      processSelect(query, expectedResult, actualResult)
    } else if (query.isAskType) {
      processAsk(query, expectedResult, actualResult)
    } else if (query.isConstructType || query.isDescribeType) {
      processGraph(query, expectedResult, actualResult)
    } else {
      fail(s"unsupported query type: ${query.queryType().name()}")
    }
  }

//  def runQueries(queries: Seq[Query], data: Model): Seq[SPARQLResult]

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

  private def processGraph(query: Query, resultExpected: SPARQLResult, resultActual: SPARQLResult) = {
    if (query.isConstructQuad) {
      import org.apache.jena.sparql.util.IsoMatcher
      try {
        if (!resultExpected.isDataset) fail("Expected results are not a graph: ")
        val resultsExpected = resultExpected.getDataset
        if (!IsoMatcher.isomorphic(resultsExpected.asDatasetGraph, resultActual.getDataset.asDatasetGraph())) {
          fail("Results do not match: ")
        }
      } catch {
        case ex: Exception =>
          val typeName = if (query.isConstructType) "construct" else "describe"
          fail("Exception in result testing (" + typeName + "): " + ex)
      }
    }
    else {
      try {
        if (!resultExpected.isGraph) fail("Expected results are not a graph: ")
        if (!resultExpected.getModel.isIsomorphicWith(resultActual.getModel)) {
          import org.apache.jena.util.FileUtils
          val out = FileUtils.asPrintWriterUTF8(System.out)
          out.println("=======================================")
          out.println("Failure:")
          out.println(s"Query:\n$query")
          out.println("expected:")
          resultExpected.getModel.write(out, "TTL")
          out.println("---------------------------------------")
          out.println("got:")
          resultActual.getModel.write(out, "TTL")
          fail("Results do not match: " + query)
        }
      } catch {
        case ex: Exception =>
          val typeName = if (query.isConstructType) "construct"
          else "describe"
          fail("Exception in result testing (" + typeName + "): " + ex)
      }
    }
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

  def readQueryString(queryFileURL: String): String = {
    println(s"loading query from $queryFileURL")
    val is = StreamManager.get().open(queryFileURL)
    withResources[AutoCloseable, String](is)(_ => {
      scala.io.Source.fromInputStream(is).mkString
    })
  }

  private def readExpectedResult(resultFileURL: String): SPARQLResult = {
    println(s"loading expected result from $resultFileURL")
    val format = ResultsFormat.guessSyntax(resultFileURL)

    val is = StreamManager.get().open(resultFileURL)

    // CONSTRUCT or DESCRIBE
    if (ResultsFormat.isRDFGraphSyntax(format)) {
      val m = loadData(resultFileURL)
      return new SPARQLResult(m)
    }

    // SELECT or ASK result
    ReadAnything.read(resultFileURL)
  }

  def loadData(datasetURL: String): Model = {
    println(s"loading data from $datasetURL")
    import java.io.IOException
    import java.net.MalformedURLException
    try {
      val is = StreamManager.get().open(datasetURL)

      val data = withResources[AutoCloseable, Model](is)(_ => {
        val model = ModelFactory.createDefaultModel()
        RDFDataMgr.read(model, is, null, if (datasetURL.endsWith(".rdf")) Lang.RDFXML else Lang.TURTLE)
        model
      })
      data.setNsPrefix("", "http://www.example.org/")
      println("Data:")
      data.write(System.out, "Turtle")
      data
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
