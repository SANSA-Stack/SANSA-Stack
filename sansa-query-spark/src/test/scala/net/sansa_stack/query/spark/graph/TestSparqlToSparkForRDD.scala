package net.sansa_stack.query.spark.graph

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.query.spark.graph.jena.{Ops, SparqlParser}
import net.sansa_stack.query.spark.graph.jena.model.{Config, IntermediateResult, SparkExecutionModel}
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

class TestSparqlToSparkForRDD extends FunSuite with DataFrameSuiteBase {

  test("read query 1 and run the query") {

    Config.setInputGraphFile("sansa-query-spark/src/test/resources/Clustering_sampledata.nt")
      .setInputQueryFile("sansa-query-spark/src/test/resources/queries/query1.txt")
      .setLang(Lang.NTRIPLES)
    SparkExecutionModel.setSparkSession(spark)
    SparkExecutionModel.loadGraph()
    val sp = new SparqlParser(Config.getInputQueryFile)
    var ops: Ops = null
    // BPG Matching
    ops = sp.getOps.dequeue()
    ops.execute()
    assert(ops.getTag.equals("Bgp Match") && IntermediateResult.getResult(ops.getId).count() == 6)
    // Filter
    ops = sp.getOps.dequeue()
    ops.execute()
    assert(ops.getTag.equals("FILTER") && IntermediateResult.getResult(ops.getId).count() == 3)
  }
}
