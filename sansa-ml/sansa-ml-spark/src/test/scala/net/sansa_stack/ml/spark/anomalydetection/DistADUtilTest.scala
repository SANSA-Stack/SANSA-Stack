package net.sansa_stack.ml.spark.anomalydetection

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.jena.sys.JenaSystem
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

/**
  * Test class for @link{DistADUtil}
  */
class DistADUtilTest extends AnyFunSuite with SharedSparkContext {

  System.setProperty(
    "spark.serializer",
    "org.apache.spark.serializer.KryoSerializer"
  )
  System.setProperty(
    "spark.kryo.registrator", String.join(",",
      "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
      "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify",
      "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator")
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
      String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify",
        "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator"
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

  test("Literal String isNumeric test") {
    var stringValue = "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>"
    assert(DistADUtil.isNumeric(stringValue) == true)
    stringValue = "\"2-3\"^^<http://www.w3.org/2001/XMLSchema#integer>"
    assert(DistADUtil.isNumeric(stringValue) == false)
  }

  test("Test isAllDigits") {
    var stringValue = "12341231313123"
    assert(DistADUtil.isAllDigits(stringValue) == true)
    stringValue = "1231231a"
    assert(DistADUtil.isAllDigits(stringValue) == false)
    stringValue = "1231231-123123123"
    assert(DistADUtil.isAllDigits(stringValue) == false)
    stringValue = "123123112#34434"
    assert(DistADUtil.isAllDigits(stringValue) == false)
  }

  test("Test searchEdge") {
    val objList = List(
      "http://www.w3.org/2001/XMLSchema#decimal",
      "http://www.w3.org/2001/XMLSchema#integer"
    )
    var stringValue = "\"2\"^^http://www.w3.org/2001/XMLSchema#integer"
    assert(DistADUtil.searchEdge(stringValue, objList) == true)
    stringValue = "\"2-3\"^^http://www.w3.org/2001/XMLSchema#String"
    assert(DistADUtil.searchEdge(stringValue, objList) == false)
  }

  test("Test getNumber") {
    var stringValue = "2^^http://www.w3.org/2001/XMLSchema#integer"
    assert(DistADUtil.getNumber(stringValue).equals(2.0))
    stringValue = "23^^http://www.w3.org/2001/XMLSchema#String"
    assert(DistADUtil.getNumber(stringValue).equals(23.0))
    stringValue = "333.12^^http://www.w3.org/2001/XMLSchema#String"
    assert(DistADUtil.getNumber(stringValue).equals(333.12))
    stringValue = "\"16\"^^<http://www.w3.org/2001/XMLSchema#integer>"
    assert(DistADUtil.getNumber(stringValue).equals(16.0))
    stringValue = "\"1\"6\"^^<http://www.w3.org/2001/XMLSchema#integer>"
    assert(DistADUtil.getNumber(stringValue).equals(Double.NaN))
  }

  test("Test getOnlyLiteralObjects") {
    val data = DistADUtil.readData(spark, dataPath)
    val onlyLiterals = DistADUtil.getOnlyLiteralObjects(data)
    assert(data.count() == 13)
    assert(onlyLiterals.count() == 6)
  }

  test("Test triplesWithNumericLit") {
    val data = DistADUtil.readData(spark, dataPath)
    val onlyNumericLiterals = DistADUtil.triplesWithNumericLit(data)
    assert(data.count() == 13)
    assert(onlyNumericLiterals.count() == 3)
  }

  test("Test triplesWithNumericLitWithTypeIgnoreEndingWithID") {
    val data = DistADUtil.readData(spark, dataPath)
    val onlyNumericLiterals =
      DistADUtil.triplesWithNumericLitWithTypeIgnoreEndingWithID(data)
    assert(data.count() == 13)
    assert(onlyNumericLiterals.count() == 3)
  }

  test("Test createDF") {
    val data = DistADUtil.readData(spark, dataPath)
    assert(DistADUtil.createDF(data).count() == 13)
  }

  test("Test createDFWithConversion") {
    val data = DistADUtil.readData(spark, dataPath)
    val triplesWithNumericLit = DistADUtil.triplesWithNumericLit(data)
    assert(
      DistADUtil.createDFWithConversion(triplesWithNumericLit).count() == 3
    )
  }

}
