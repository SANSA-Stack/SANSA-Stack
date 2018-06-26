package net.sansa_stack.query.spark.graph

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.query.spark.graph.jena.{Ops, SparqlParser}
import net.sansa_stack.query.spark.graph.jena.model.{Config, IntermediateResult, SparkExecutionModel}
import net.sansa_stack.query.spark.graph.jena.util.{BasicGraphPattern, MatchSet}
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

class TestSparqlToSparkForRDD extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.io._
  import net.sansa_stack.rdf.spark.model.graph._

  test("bgp matching test") {
    val path = "sansa-query-spark/src/test/resources/S2X.nt"
    val lang = Lang.NTRIPLES
    val graph = spark.rdf(lang)(path).asGraph().cache()

    val pattern1 = Triple.create(NodeFactory.createVariable("A"),
      NodeFactory.createURI("http://twitter/knows"),
      NodeFactory.createVariable("B"))
    val pattern2 = Triple.create(NodeFactory.createVariable("A"),
      NodeFactory.createURI("http://twitter/likes"),
      NodeFactory.createVariable("B"))
    val pattern3 = Triple.create(NodeFactory.createVariable("B"),
      NodeFactory.createURI("http://twitter/knows"),
      NodeFactory.createVariable("C"))
    val bgp = new BasicGraphPattern(Iterator(pattern1, pattern2, pattern3))
    val patterns = spark.sparkContext.broadcast(bgp.triplePatterns)

    val candidateGraph = MatchSet.createCandidateGraph(graph, patterns).cache()
    graph.unpersist()
    val localGraph = MatchSet.localMatch(candidateGraph, patterns).cache()
    candidateGraph.unpersist()
    val mergedGraph = MatchSet.joinNeighbourCandidate(localGraph).cache()
    localGraph.unpersist()
    val finalGraph = MatchSet.remoteMatch(mergedGraph).cache()
    mergedGraph.unpersist()
    SparkExecutionModel.createSparkSession(spark)
    val finalResult = MatchSet.generateResultRDD(finalGraph, patterns, spark).cache()
    assert(finalResult.count()==1)
  }

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
    // Order By
    ops = sp.getOps.dequeue()
    ops.execute()
    assert(ops.getTag.equals("ORDER BY") && IntermediateResult.getResult(ops.getId).count() == 3)
    // Select
    ops = sp.getOps.dequeue()
    ops.execute()
    assert(ops.getTag.equals("SELECT") && IntermediateResult.getResult(ops.getId).first().getField.size == 2)
  }

  test("read query 2 and run the query") {
    Config.setInputGraphFile("sansa-query-spark/src/test/resources/Clustering_sampledata.nt")
      .setInputQueryFile("sansa-query-spark/src/test/resources/queries/query2.txt")
      .setLang(Lang.NTRIPLES)
    SparkExecutionModel.setSparkSession(spark)
    SparkExecutionModel.loadGraph()
    val sp = new SparqlParser(Config.getInputQueryFile)
    var ops: Ops = null
    // BGP Matching
    ops = sp.getOps.dequeue()
    ops.execute()
    assert(ops.getTag.equals("Bgp Match") && IntermediateResult.getResult(ops.getId).count() == 13)
    // Right Side BGP Matching
    ops = sp.getOps.dequeue()
    ops.execute()
    assert(ops.getTag.equals("Bgp Match") && IntermediateResult.getResult(ops.getId).count() == 6)
    // Left Join
    ops = sp.getOps.dequeue()
    ops.execute()
    assert(ops.getTag.equals("OPTIONAL") && IntermediateResult.getResult(ops.getId).count() == 13)
    // Right Side BGP Matching 2
    ops = sp.getOps.dequeue()
    ops.execute()
    assert(ops.getTag.equals("Bgp Match") && IntermediateResult.getResult(ops.getId).count() == 8)
    // Left Join
    ops = sp.getOps.dequeue()
    ops.execute()
    assert(ops.getTag.equals("OPTIONAL") && IntermediateResult.getResult(ops.getId).count() == 13)
    // Filter
    ops = sp.getOps.dequeue()
    ops.execute()
    assert(ops.getTag.equals("FILTER") && IntermediateResult.getResult(ops.getId).count() == 10)
  }
}
