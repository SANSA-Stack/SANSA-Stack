package net.sansa_stack.query.spark.datalake

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.funsuite.AnyFunSuite

class TestDataLakeEngine extends AnyFunSuite with DataFrameSuiteBase {

  import net.sansa_stack.query.spark._

  val configFile = getClass.getResource("/datalake/config_csv-only").getPath
  val mappingsFile = getClass.getResource("/datalake/mappings_csv-only.ttl").getPath

  test("running BSBM Q1 should result 10") {

    val query = getClass.getResource("/datalake/queries/Q1.sparql").getPath
    val result = spark.sparqlDL(query, mappingsFile, configFile)

    val size = result.count()

    assert(size == 10)
  }

  test("running BSBM Q2 should result 200") {

    val query = getClass.getResource("/datalake/queries/Q2.sparql").getPath
    val result = spark.sparqlDL(query, mappingsFile, configFile)

    val size = result.count()

    assert(size == 200)
  }

  test("running BSBM Q3 should result 0") {

    val query = getClass.getResource("/datalake/queries/Q3.sparql").getPath
    val result = spark.sparqlDL(query, mappingsFile, configFile)

    val size = result.count()

    assert(size == 0)
  }

  test("running BSBM Q4 should result 7") {

    val query = getClass.getResource("/datalake/queries/Q4.sparql").getPath
    val result = spark.sparqlDL(query, mappingsFile, configFile)

    val size = result.count()

    assert(size == 7)
  }

  test("running BSBM Q5 should result 0") {

    val query = getClass.getResource("/datalake/queries/Q5.sparql").getPath
    val result = spark.sparqlDL(query, mappingsFile, configFile)

    val size = result.count()

    assert(size == 0)
  }

  test("running BSBM Q6 should result 0") {

    val query = getClass.getResource("/datalake/queries/Q6.sparql").getPath
    val result = spark.sparqlDL(query, mappingsFile, configFile)

    val size = result.count()

    assert(size == 0)
  }

}
