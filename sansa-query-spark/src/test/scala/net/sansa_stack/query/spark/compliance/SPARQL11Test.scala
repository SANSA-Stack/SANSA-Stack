package net.sansa_stack.query.spark.compliance

import java.io.InputStream
import java.net.{JarURLConnection, URL}
import java.nio.file.Files
import java.util.Properties

import scala.collection.JavaConverters._

import com.google.common.collect.ImmutableSet
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import it.unibz.inf.ontop.test.sparql.ManifestTestUtils
import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

import net.sansa_stack.query.spark.sparql2sql.Sparql2Sql
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.utils.Logging

/**
 * @author Lorenz Buehmann
 */
class SPARQL11Test
  extends FunSuite
    with DataFrameSuiteBase
    with Logging {

  private val aggregatesManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#"
  private val bindManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/bind/manifest#"
  private val bindingsManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/bindings/manifest#"
  private val functionsManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/functions/manifest#"
  private val constructManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/construct/manifest#"
  private val csvTscResManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/csv-tsv-res/manifest#"
  private val groupingManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/grouping/manifest#"
  private val negationManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/negation/manifest#"
  private val existsManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/exists/manifest#"
  private val projectExpressionManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/project-expression/manifest#"
  private val propertyPathManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/property-path/manifest#"
  private val subqueryManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/subquery/manifest#"
  private val serviceManifest = "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service/manifest#"

  private val IGNORE = ImmutableSet.of(/* AGGREGATES */
    // TODO: support GROUP_CONCAT
    aggregatesManifest + "agg-groupconcat-01", aggregatesManifest + "agg-groupconcat-02", aggregatesManifest + "agg-groupconcat-03", // TODO: support IF
    aggregatesManifest + "agg-err-02", /* BINDINGS
			 */
    // TODO: fix it (UNDEF involves the notion of COMPATIBILITY when joining)
    bindingsManifest + "values8", bindingsManifest + "values5", /* FUNCTIONS */
    // bnode not supported in SPARQL transformation
    functionsManifest + "bnode01", functionsManifest + "bnode02", // the SI does not preserve the original timezone
    functionsManifest + "hours", functionsManifest + "day", // not supported in SPARQL transformation
    functionsManifest + "if01", functionsManifest + "if02", functionsManifest + "in01", functionsManifest + "in02", functionsManifest + "iri01", // not supported in H2 transformation
    functionsManifest + "md5-01", functionsManifest + "md5-02", // The SI does not support IRIs as ORDER BY conditions
    functionsManifest + "plus-1", functionsManifest + "plus-2", // SHA1 is not supported in H2
    functionsManifest + "sha1-01", functionsManifest + "sha1-02", // SHA512 is not supported in H2
    functionsManifest + "sha512-01", functionsManifest + "sha512-02", functionsManifest + "strdt01", functionsManifest + "strdt02", functionsManifest + "strdt03", functionsManifest + "strlang01", functionsManifest + "strlang02", functionsManifest + "strlang03", functionsManifest + "timezone", // TZ is not supported in H2
    functionsManifest + "tz", /* CONSTRUCT not supported yet */
    // Projection cannot be cast to Reduced in rdf4j
    constructManifest + "constructwhere01", constructManifest + "constructwhere02", constructManifest + "constructwhere03", // problem importing dataset
    constructManifest + "constructwhere04", /* CSV */
    // Sorting by IRI is not supported by the SI
    csvTscResManifest + "tsv01", csvTscResManifest + "tsv02", // different format for number and not supported custom datatype
    csvTscResManifest + "tsv03", /* GROUPING */
    // Multi-typed COALESCE as grouping condition TODO: support it
    groupingManifest + "group04", /* NEGATION
			not supported yet */ negationManifest + "subset-by-exclusion-nex-1", negationManifest + "temporal-proximity-by-exclusion-nex-1", negationManifest + "subset-01", negationManifest + "subset-02", negationManifest + "set-equals-1", negationManifest + "subset-03", negationManifest + "exists-01", negationManifest + "exists-02", // DISABLED DUE TO ORDER OVER IRI
    negationManifest + "full-minuend", negationManifest + "partial-minuend", // TODO: enable it
    negationManifest + "full-minuend-modified", negationManifest + "partial-minuend-modified", /* EXISTS
			not supported yet */ existsManifest + "exists01", existsManifest + "exists02", existsManifest + "exists03", existsManifest + "exists04", existsManifest + "exists05", /* PROPERTY PATH */
    // Not supported: ArbitraryLengthPath
    propertyPathManifest + "pp02", // wrong result, unexpected binding
    propertyPathManifest + "pp06", propertyPathManifest + "pp12", propertyPathManifest + "pp14", propertyPathManifest + "pp16", propertyPathManifest + "pp21", propertyPathManifest + "pp23", propertyPathManifest + "pp25", // Not supported: ZeroLengthPath
    propertyPathManifest + "pp28a", propertyPathManifest + "pp34", propertyPathManifest + "pp35", propertyPathManifest + "pp36", propertyPathManifest + "pp37", /* SERVICE
			not supported yet */ serviceManifest + "service1", // no loading of the dataset
    serviceManifest + "service2", serviceManifest + "service3", serviceManifest + "service4a", serviceManifest + "service5", serviceManifest + "service6", serviceManifest + "service7", /* SUBQUERY
			*/
    // Quad translated as a triple. TODO: fix it
    subqueryManifest + "subquery02", subqueryManifest + "subquery04", // EXISTS is not supported yet
    subqueryManifest + "subquery10", // ORDER BY IRI (for supported by the SI)
    subqueryManifest + "subquery11", // unbound variable: Var TODO: fix it
    subqueryManifest + "subquery12", subqueryManifest + "subquery13", // missing results (TODO: fix)
    subqueryManifest + "subquery14")




  var ontopProperties: Properties = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    ontopProperties = new Properties()
    ontopProperties.load(getClass.getClassLoader.getResourceAsStream("ontop-spark.properties"))
  }


  val testData = ManifestTestUtils.parametersFromSuperManifest("/testcases-dawg-sparql-1.1/manifest-all.ttl", IGNORE)

  testData.asScala
    .filter(data => !IGNORE.contains(data(0).asInstanceOf[String]))
    .slice(0, 100)
//    .filter(data => data(1) == "Aggregates-\"AVG\"")
    .foreach { d =>

    val queryFileURL = d(2).asInstanceOf[String]
    val dataset = d(4).asInstanceOf[org.eclipse.rdf4j.query.impl.SimpleDataset]
    val testName = d(1).asInstanceOf[String]

    test(s"testing $testName") {
      val queryString = readQueryString(queryFileURL)
      println(s"SPARQL query: $queryString")

      // load data
      val datasetURL = dataset.getDefaultGraphs.iterator().next().toString
      val triples = loadData(spark, datasetURL)

      // convert to SQL
      val sql = Sparql2Sql.asSQL(spark, queryString, triples, ontopProperties)
        .replace("\"", "`")
        .replace("`PUBLIC`.", "")
      println(s"SQL query: $sql")

      // run query
      val result = spark.sql(sql)
      result.show(false)
      result.printSchema()
    }

  }

  def readQueryString(queryFileURL: String): String = {
    val is = new URL(queryFileURL).openStream

    scala.io.Source.fromInputStream(is).mkString
  }

  def loadData(spark: SparkSession, datasetURL: String): RDD[Triple] = {
    import java.io.IOException
    import java.net.MalformedURLException
    try {
      val url = new URL(datasetURL)
      val conn = url.openConnection.asInstanceOf[JarURLConnection]
      val in = conn.getInputStream
      val data = ModelFactory.createDefaultModel()
      RDFDataMgr.read(data, in, null, Lang.TURTLE)
      data.write(System.out, "N-Triples")

      spark.sparkContext.parallelize(data.getGraph.find().toList.asScala)

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
