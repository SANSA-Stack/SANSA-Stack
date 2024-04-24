package net.sansa_stack.query.spark.compliance

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.query.spark.api.domain.{QueryEngineFactory, QueryExecutionFactorySpark}
import net.sansa_stack.query.tests.W3CConformanceSPARQLQueryEvaluationTestSuiteRunner
import net.sansa_stack.rdf.spark.utils.SparkSessionUtils
import org.apache.jena.graph.Triple
import org.apache.jena.query.Query
import org.apache.jena.rdf.model.Model
import org.apache.jena.sparql.resultset.SPARQLResult
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.scalatest.Suite

import scala.collection.JavaConverters._

/**
 * SPARQL 1.1 test suite runner on Apache Spark.
 *
 *
 * @author Lorenz Buehmann
 */
abstract class SPARQL11TestSuiteRunnerSpark
  extends W3CConformanceSPARQLQueryEvaluationTestSuiteRunner
    with org.scalatest.BeforeAndAfterAllConfigMap
    with DataFrameSuiteBase { self: Suite =>

  override val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected: Boolean = false

  override def conf: SparkConf = {
    super.conf
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", String.join(
        ", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator"))
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
  }
  override def afterAll(): Unit = {
    super.afterAll()
  }

  def getEngineFactory: QueryEngineFactory

  lazy val engineFactory: QueryEngineFactory = getEngineFactory

  val db = "TEST"

  var qef: QueryExecutionFactorySpark = _
  var previousDataURL: String = _
  var data: Model = _
  var dataRDD: RDD[Triple] = _

  override def loadData(datasetURL: String): Model = {
    if (datasetURL != previousDataURL) {
      previousDataURL = datasetURL

      data = super.loadData(datasetURL)

      // we drop the Spark database to remove all tables from previous loaded data
      spark.sql(s"DROP DATABASE IF EXISTS $db CASCADE")
      SparkSessionUtils.clearAllTablesAndViews(spark)

      // we cannot handle empty data
      if (!data.isEmpty) {
        // distribute on Spark
        dataRDD = spark.sparkContext.parallelize(data.getGraph.find().toList.asScala)

        // we create a Spark database here to keep the implicit partitioning separate
        spark.sql(s"CREATE DATABASE IF NOT EXISTS $db")
        spark.sql(s"USE $db")
        qef = engineFactory.create(dataRDD)
      }

      data
    } else {
      data
    }
  }

  override def runQuery(query: Query, data: Model): SPARQLResult = {
    val qe = qef.createQueryExecution(query)

    // produce result based on query type
    val result = if (query.isSelectType) { // SELECT
      val rs = qe.execSelect()
      new SPARQLResult(rs)
    } else if (query.isAskType) { // ASK
      val b = qe.execAsk()
      new SPARQLResult(b)
    } else if (query.isConstructType) { // CONSTRUCT
      val triples = qe.execConstruct()
      new SPARQLResult(triples)
    } else { // DESCRIBE todo
      fail("unsupported query type: DESCRIBE")
      null
    }
    // clean up

    qe.close()

    result
  }

  super.beforeAll()
}
