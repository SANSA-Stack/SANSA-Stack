package net.sansa_stack.query.spark

import com.holdenkarau.spark.testing.SharedSparkContext
import net.sansa_stack.query.spark.api.domain.{QueryEngineFactory, QueryExecutionFactorySpark}
import net.sansa_stack.query.tests.{SPARQLQueryEvaluationTestSuite, SPARQLQueryEvaluationTestSuiteRunner}
import org.apache.jena.graph.Triple
import org.apache.jena.query.Query
import org.apache.jena.rdf.model.Model
import org.apache.jena.sparql.resultset.SPARQLResult
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{ConfigMap, Suite}

import scala.jdk.CollectionConverters._

/**
 * SPARQL test suite runner on Apache Spark.
 *
 *
 * @author Lorenz Buehmann
 */
abstract class SPARQLTestSuiteRunnerSpark(testSuite: SPARQLQueryEvaluationTestSuite)
  extends SPARQLQueryEvaluationTestSuiteRunner(testSuite)
//
    with org.scalatest.BeforeAndAfterAllConfigMap
    with SharedSparkContext { self: Suite =>

  System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  System.setProperty("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")

  @transient private var _spark: SparkSession = _

  lazy val spark = SparkSession.builder().config(
    conf
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")
  ).getOrCreate()


  override val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected: Boolean = false

  override def beforeAll(configMap: ConfigMap): Unit = {
    conf.set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")
    _spark = SparkSession.builder().config(conf).master("local[1]").getOrCreate()

    val toIgnore = configMap.get("ignore")
    if (toIgnore.nonEmpty) {
      toIgnore.get.asInstanceOf[String].split(",").map(_.trim).foreach(name => IGNORE_NAMES.add(name))
    }
  }

  override def afterAll(configMap: ConfigMap): Unit = {
    super.afterAll()
    spark.stop()
    _spark = null
  }

  def getEngineFactory: QueryEngineFactory

  lazy val engineFactory: QueryEngineFactory = getEngineFactory

  val db = "TEST"

  var previousModel: Model = _
  var triplesRDD: RDD[Triple] = _
  var qef: QueryExecutionFactorySpark = _

  override def runQuery(query: Query, data: Model): SPARQLResult = {
    // do some caching here to avoid reloading the same data
    if (data != previousModel) {
      // we drop the Spark database to remove all tables
      spark.sql(s"DROP DATABASE IF EXISTS $db CASCADE")

      // distribute on Spark
      triplesRDD = spark.sparkContext.parallelize(data.getGraph.find().asScala.toList)

      // we create a Spark database here to keep the implicit partitioning separate

      spark.sql(s"CREATE DATABASE IF NOT EXISTS $db")
      spark.sql(s"USE $db")

      qef = engineFactory.create(triplesRDD)

      previousModel = data
    }

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
}
