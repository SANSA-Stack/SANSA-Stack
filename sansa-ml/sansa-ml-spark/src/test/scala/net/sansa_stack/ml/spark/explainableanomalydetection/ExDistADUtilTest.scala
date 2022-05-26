package net.sansa_stack.ml.spark.explainableanomalydetection

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.jena.sys.JenaSystem
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

/**
  * Test class for @link{ExDistADUtil}
  */
class ExDistADUtilTest extends AnyFunSuite with SharedSparkContext {

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
    .appName(s"Unit Test")
    .config(
      "spark.serializer",
      "org.apache.spark.serializer.KryoSerializer"
    )
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

  override def beforeAll() {
    super.beforeAll()
    JenaSystem.init()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.close()
  }

  test("IQR Test 1") {
    val data: Array[Double] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100)
    val result = ExDistADUtil.iqr(data, 1)
    assert(result.length == 1)
  }

  test("IQR Test 2") {
    val data: Array[Double] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val result = ExDistADUtil.iqr(data, 1)
    assert(result.length == 0)
  }

  test("MAD Test 1") {
    val data: Array[Double] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100)
    val result = ExDistADUtil.mad(data, 1)
    assert(result.length == 1)
  }

  test("MAD Test 2") {
    val data: Array[Double] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val result = ExDistADUtil.mad(data, 1)
    assert(result.length == 0)
  }

  test("Z-Score Test 1") {
    val data: Array[Double] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100)
    val result = ExDistADUtil.mad(data, 1)
    assert(result.length == 1)
  }

  test("Z-Score Test 2") {
    val data: Array[Double] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val result = ExDistADUtil.mad(data, 1)
    assert(result.length == 0)
  }

  test("Test createDF") {
    val data = ExDistADUtil.readData(spark, dataPath)
    assert(ExDistADUtil.createDF(data).count() == 13)
  }

}
