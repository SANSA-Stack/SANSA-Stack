package net.sansa_stack.query.spark.compliance

import java.io.InputStreamReader
import java.net.{JarURLConnection, URL}
import java.util

import scala.collection.JavaConverters._

import com.google.common.collect.ImmutableSet
import it.unibz.inf.ontop.test.sparql.ManifestTestUtils
import org.apache.jena.query._
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.resultset.rw.ResultsStAX
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.jena.sparql.resultset.{ResultSetCompare, SPARQLResult}
import org.scalatest.FunSuite

import net.sansa_stack.rdf.spark.utils.Logging

/**
 * SPARQL 1.1 test suite.
 *
 * @author Lorenz Buehmann
 */
abstract class SPARQL11TestSuite
  extends FunSuite
    with Logging {

  protected val aggregatesManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#"
  protected  val bindManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/bind/manifest#"
  protected  val bindingsManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/bindings/manifest#"
  protected  val functionsManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/functions/manifest#"
  protected  val constructManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/construct/manifest#"
  protected  val csvTscResManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/csv-tsv-res/manifest#"
  protected  val groupingManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/grouping/manifest#"
  protected  val negationManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/negation/manifest#"
  protected  val existsManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/exists/manifest#"
  protected  val projectExpressionManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/project-expression/manifest#"
  protected  val propertyPathManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/property-path/manifest#"
  protected  val subqueryManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/subquery/manifest#"
  protected  val serviceManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service/manifest#"

  // contains the list of ignored tests cases, must be overridden
  lazy val IGNORE: ImmutableSet[String] = ImmutableSet.of()

  val testData: util.Collection[Array[AnyRef]] = ManifestTestUtils.parametersFromSuperManifest("/testcases-dawg-sparql-1.1/manifest-all.ttl", IGNORE)

  // the main loop over the test data starts here
  // single ScalaTest is generated per query
  testData.asScala
    .filter(data => !IGNORE.contains(data(0).asInstanceOf[String]))
    .slice(0, 200)
    .filter(data => data(1) == "Sub query-\"sq01 - Subquery within graph pattern\"")
//    .filter(data => data(1) == "BIND-\"bind05 - BIND\"")
    //    .filter(data => data(1) == "Aggregates-\"AVG\"")
    .foreach { d =>

      // get the relevant data from the parsed array
      val queryFileURL = d(2).asInstanceOf[String]
      val resultFileURL = d(3).asInstanceOf[String]
      val dataset = d(4).asInstanceOf[org.eclipse.rdf4j.query.impl.SimpleDataset]
      val testName = d(1).asInstanceOf[String]

      // test starts here
      test(s"testing $testName") {
        val queryString = readQueryString(queryFileURL)
        val query = QueryFactory.create(queryString)
        println(s"SPARQL query:\n $query")

        // load data
        val dg = dataset.getDefaultGraphs
        val datasetURL =
          if (!dg.isEmpty) {
            dataset.getDefaultGraphs.iterator().next()
          } else {
            dataset.getNamedGraphs.iterator().next()
          }
        val data = loadData(datasetURL.toString)
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
    val is = new URL(queryFileURL).openStream

    scala.io.Source.fromInputStream(is).mkString
  }

  private def readExpectedResult(resultFileURL: String): SPARQLResult = {
    val is = new URL(resultFileURL).openStream
    ResultsStAX.read(is, ModelFactory.createDefaultModel, null)

    //    ResultSetFactory.load(is, ResultsFormat.FMT_RS_XML)
  }

  private def loadData(datasetURL: String): Model = {
    println(s"loading data from $datasetURL")
    import java.io.IOException
    import java.net.MalformedURLException
    try {
      val url = new URL(datasetURL)
      val conn = url.openConnection.asInstanceOf[JarURLConnection]
      val in = conn.getInputStream

      val data = ModelFactory.createDefaultModel()
      RDFDataMgr.read(data, in, null, if (datasetURL.endsWith(".rdf")) Lang.RDFXML else Lang.TURTLE)
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
