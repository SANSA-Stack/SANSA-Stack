package net.sansa_stack.query.tests

import java.net.URL

import net.sansa_stack.query.tests.util._
import org.apache.jena.query._
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.resultset.ResultSetLang
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat, ResultSetMgr}
import org.apache.jena.sparql.resultset.{ResultSetCompare, ResultsFormat, SPARQLResult}
import org.scalatest.FunSuite

import scala.collection.mutable



/**
 * SPARQL test suite runner.
 *
 * Inheriting classes have to implement the method
 * [[net.sansa_stack.query.tests.SPARQLQueryEvaluationTestSuiteRunner#runQuery(org.apache.jena.query.Query, org.apache.jena.rdf.model.Model)]] method.
 *
 * @param sparqlVersion the test suite
 * @author Lorenz Buehmann
 */
abstract class SPARQLQueryEvaluationTestSuiteRunner(val testSuite: SPARQLQueryEvaluationTestSuite)
  extends FunSuite {

  // contains the list of ignored tests case IDs, must be overridden
  lazy val IGNORE: Set[String] = Set.empty[String]

  // an optional filter function to ignore test cases
  val IGNORE_NAMES: mutable.Set[String] = mutable.Set[String]()

  // an optional filter function to ignore test cases
  lazy val IGNORE_FILTER: SPARQLQueryEvaluationTest => Boolean = _ => {true}

  // holds the test data
  val testData: List[SPARQLQueryEvaluationTest] = testSuite.tests

  // the main loop over the test data starts here
  // a single ScalaTest is generated per query
  testData
    .filter(data => !IGNORE.contains(data.uri))
    .filter(t => IGNORE_NAMES.isEmpty || IGNORE_NAMES.exists(t.name.startsWith))
    .filter(IGNORE_FILTER)
    .groupBy(_.dataFile)
    .foreach { case (dataFile, tests) =>
      // load data
      val data = loadData(dataFile)
      data.setNsPrefix("", "http://www.example.org/")
//      println("Data:")
//      data.write(System.out, "Turtle")

      tests.sortBy(_.queryFile)
        // .filter(testCase => testCase.name == "issue15-q1")
        .foreach(testCase => {
        // get the relevant data from the test case
        val queryFileURL = testCase.queryFile
        val resultFileURL = testCase.resultsFile
        val testName = testCase.name

        // test starts here
        test(s"testing $testName") {
          val queryString = readQueryString(queryFileURL)
          val query = QueryFactory.create(queryString)
          println(s"Test $testName: SPARQL query:\n $query")

          // run the SPARQL query
          val actualResult = runQuery(query, data)

          // read expected result
          val expectedResult: Option[SPARQLResult] = resultFileURL.map(readExpectedResult)

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
      })
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

  private def processAsk(query: Query, resultExpected: Option[SPARQLResult], resultActual: SPARQLResult) = {

    if (resultExpected.isEmpty) {
      ResultSetMgr.write(System.out, resultActual.getBooleanResult, ResultSetLang.RS_XML)
      fail("No expected result defined - actual output printed to console")
    } else {
      assert(resultActual.getBooleanResult == resultExpected.get.getBooleanResult, "Result of ASK query does not match")
    }
  }

  private def processGraph(query: Query, resultExpectedOpt: Option[SPARQLResult], resultActual: SPARQLResult) = {

    if (resultExpectedOpt.isEmpty) {
      if (query.isConstructQuad) {
        RDFDataMgr.write(System.out, resultActual.getDataset, RDFFormat.TRIG_PRETTY)
      } else {
        RDFDataMgr.write(System.out, resultActual.getModel, RDFFormat.TURTLE_PRETTY)
        fail("No expected result defined - actual output printed to console")
      }
    } else {
      val resultExpected = resultExpectedOpt.get


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
  }

  private def processSelect(query: Query, resultsOpt: Option[SPARQLResult], resultsAct: SPARQLResult) = {
    val resultsActual = ResultSetFactory.makeRewindable(resultsAct.getResultSet)

    if (resultsOpt.isEmpty) {
      ResultSetMgr.write(System.out, resultsActual, ResultSetLang.RS_XML)
      fail("No expected result defined - actual output printed to console")
    } else {
      val results = resultsOpt.get

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
      val b = ResultSetCompareUtils.resultSetEquivalent(query, resultsActual, resultsExpected)

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
    val fileURL = new URL(resultFileURL).toString

    val format = ResultsFormat.guessSyntax(fileURL)

    // CONSTRUCT or DESCRIBE
    if (ResultsFormat.isRDFGraphSyntax(format)) {
      val m = RDFDataMgr.loadModel(fileURL)
      return new SPARQLResult(m)
    }

    // SELECT or ASK result
    ResultSetFactory.result(fileURL)
  }

  private def loadData(datasetURL: String): Model = {
    println(s"loading data from $datasetURL")
    import java.io.IOException
    import java.net.MalformedURLException
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
