package net.sansa_stack.spark.query.graph

import net.sansa_stack.query.spark.graph.jena.SparqlParser
import net.sansa_stack.query.spark.graph.jena.patternOp.PatternOp
import net.sansa_stack.query.spark.graph.jena.resultOp.ResultOp
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.graph.LoadGraph
import org.apache.jena.graph.Node
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import scala.io.Source

class TestSparqlToSpark extends FunSuite {

  protected val session: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("result test")
    .getOrCreate()

  test("read a N-Triple file and convert into a graph with 43 triplets") {

    val nTriplePath = "src/test/resources/Clustering_sampledata.nt"
    val triples = NTripleReader.load (session, nTriplePath)
    val size = triples.count()

    assert(size == 43)
  }

  test("read query 1 and run the query") {

    val nTriplePath = "src/test/resources/Clustering_sampledata.nt"
    val graph = LoadGraph.apply(NTripleReader.load (session, nTriplePath))

    val queryPath = "src/test/resources/queries/query1.txt"
    val sp = new SparqlParser(queryPath)

    var intermediate = Array[Map[Node, Node]]()
    // BPG Matching
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, session)
    assert(intermediate.length == 6)
    // Filter
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 3)
    // Order
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 3)
    // Select
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 3)
    // Distinct
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 3)
    // Limit
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 3)
  }

  test("read query 2 and run the query") {
    val nTriplePath = "src/test/resources/Clustering_sampledata.nt"
    val graph = LoadGraph.apply(NTripleReader.load (session, nTriplePath))

    val queryPath = "src/test/resources/queries/query2.txt"
    val sp = new SparqlParser(queryPath)

    var intermediate = Array[Map[Node, Node]]()
    // BGP Matching
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, session)
    assert(intermediate.length == 13)
    // Optional
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, session)
    assert(intermediate.length == 13)
    // Optional
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, session)
    assert(intermediate.length == 13)
    // Filter
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 3)
    // Select
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 3)
  }

  test("read query 3 and run the query") {
    val nTriplePath = "src/test/resources/Clustering_sampledata.nt"
    val graph = LoadGraph.apply(NTripleReader.load (session, nTriplePath))

    val queryPath = "src/test/resources/queries/query3.txt"
    val sp = new SparqlParser(queryPath)

    var intermediate = Array[Map[Node, Node]]()
    // BGP Matching
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, session)
    assert(intermediate.length == 13)
    // Optional
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, session)
    assert(intermediate.length == 13)
    // Optional
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, session)
    assert(intermediate.length == 13)
    // Filter
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 12)
    // Order
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 12)
    // Select
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 12)
    // Distinct
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 6)
  }

  test("read query 4 and run the query") {
    val nTriplePath = "src/test/resources/Clustering_sampledata.nt"
    val graph = LoadGraph.apply(NTripleReader.load (session, nTriplePath))

    val queryPath = "src/test/resources/queries/query4.txt"
    val sp = new SparqlParser(queryPath)

    var intermediate = Array[Map[Node, Node]]()
    // BGP Matching
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, session)
    assert(intermediate.length == 8)
    // Filter
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 3)
    // Union
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, session)
    assert(intermediate.length == 5)
    // Order
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 5)
    // Select
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 5)
    // LIMIT and OFFSET
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 2)
  }
}
