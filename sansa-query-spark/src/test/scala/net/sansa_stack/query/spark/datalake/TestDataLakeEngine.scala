package net.sansa_stack.query.spark.datalake

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite

class TestDataLakeEngine extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.query.spark.query._

  val configFile = getClass.getResource("/config_csv-only").getPath
  val mappingsFile = getClass.getResource("/mappings_csv-only.ttl").getPath

  test("running BSBM Q1 should result 23") {

    val query = getClass.getResource("/queries/Q1.sparql").getPath
    val result = spark.sparqlDL(query, mappingsFile, configFile)

    val size = result.count()

    assert(size == 23)
  }

}
