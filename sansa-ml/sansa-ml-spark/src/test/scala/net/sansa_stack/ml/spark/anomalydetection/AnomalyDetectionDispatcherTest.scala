package net.sansa_stack.ml.spark.anomalydetection

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.jena.sys.JenaSystem
import org.apache.log4j.{BasicConfigurator, Level}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

/**
  * Test class for @link{AnomalyDetectionDispatcher}
  */
class AnomalyDetectionDispatcherTest
    extends AnyFunSuite
    with SharedSparkContext {

  System.setProperty(
    "spark.serializer",
    "org.apache.spark.serializer.KryoSerializer"
  )
  System.setProperty(
    "spark.kryo.registrator",
    "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator"
  )

  lazy val spark = SparkSession
    .builder()
    .appName(s"SparqlFrame Transformer Unit Test")
    .config(
      "spark.serializer",
      "org.apache.spark.serializer.KryoSerializer"
    ) // we need Kryo serialization enabled with some custom serializers
    .config(
      "spark.kryo.registrator",
      String.join(
        ", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"
      )
    )
    .config("spark.sql.crossJoin.enabled", true)
    .getOrCreate()

  private val dataPath =
    this.getClass.getClassLoader.getResource("utils/test.ttl").getPath
  private val dataPath2 =
    this.getClass.getClassLoader.getResource("DistAD/test.ttl").getPath
  private val configFilePath =
    this.getClass.getClassLoader.getResource("DistAD/config.conf").getPath

  override def beforeAll() {
    super.beforeAll()
    JenaSystem.init()
    spark.sparkContext.setLogLevel("ERROR")
    DistADLogger.LOG.setLevel(Level.ALL)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.close()
  }

  test("NumericLiteralAnomalyDetection Test 1") {
    val data = DistADUtil.readData(spark, dataPath)
    val config: DistADConfig = new DistADConfig(configFilePath)
    val anomalyList: DataFrame =
      new NumericLiteralAnomalyDetection(spark, data, config).run()
    assert(anomalyList.count() == 0)
  }

  test("NumericLiteralAnomalyDetection Test 2") {
    val data = DistADUtil.readData(spark, dataPath)
    val config: DistADConfig = new DistADConfig(configFilePath)
    config.anomalyListSize = 1
    val anomalyList: DataFrame =
      new NumericLiteralAnomalyDetection(spark, data, config).run()
    assert(anomalyList.count() == 0)
  }

  test("CONOD Test 3") {
    val data = DistADUtil.readData(spark, dataPath)
    val config: DistADConfig = new DistADConfig(configFilePath)
    config.anomalyListSize = 1
    val anomalyList: DataFrame =
      new CONOD(spark, data, config).run()
    assert(anomalyList.count() == 0)
  }

  test("PredicateAnomalyDetection Test 4") {
    val data = DistADUtil.readData(spark, dataPath2)
    val config: DistADConfig = new DistADConfig(configFilePath)
    config.anomalyListSize = 1
    val anomalyList: DataFrame =
      new PredicateAnomalyDetection(spark, data, config).run()
    assert(anomalyList.count() == 0)
  }

  test("MultiFeatureAnomalyDetection Test 5") {

    val data = DistADUtil.readData(
      spark,
      dataPath2
    )
    val config: DistADConfig = new DistADConfig(configFilePath)
    config.maxSampleForIF = 2
    val anomalyList: DataFrame =
      new MultiFeatureAnomalyDetection(spark, data, config).run()
    assert(anomalyList.count() == 2)
  }

  test("MultiFeatureAnomalyDetection Test 6") {

    val data = DistADUtil.readData(
      spark,
      dataPath2
    )
    val config: DistADConfig = new DistADConfig(configFilePath)
    config.maxSampleForIF = 2
    config.featureExtractor = "Literal2Feature"

    val anomalyList: DataFrame =
      new MultiFeatureAnomalyDetection(spark, data, config).run()
    assert(anomalyList.count() == 2)
  }
}
