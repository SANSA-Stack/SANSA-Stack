package net.sansa_stack.query.spark.graph

import scala.io.Source

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import net.sansa_stack.query.spark.graph.jena.SparqlParser
import net.sansa_stack.query.spark.graph.jena.patternOp.PatternOp
import net.sansa_stack.query.spark.graph.jena.resultOp.ResultOp
import net.sansa_stack.query.spark.graph.jena.util.ResultFactory
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph.Node
import org.apache.jena.riot.Lang
import org.scalatest.FunSuite

class TestSparqlToSpark extends FunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.model._

  test("read a N-Triple file and convert into a graph with 43 triplets") {

    val nTriplePath = "src/test/resources/Clustering_sampledata.nt"
    val lang: Lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(nTriplePath)

    val graph = triples.asGraph()

    val size = triples.count()

    assert(size == 43)
  }

  test("read query 1 and run the query") {

    val nTriplePath = "src/test/resources/Clustering_sampledata.nt"
    val lang: Lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(nTriplePath)

    val graph = triples.asGraph()

    val queryPath = "src/test/resources/queries/query1.txt"
    val sp = new SparqlParser(queryPath)

    var intermediate = Array[Map[Node, Node]]()
    // BPG Matching
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, spark)
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
    val lang: Lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(nTriplePath)

    val graph = triples.asGraph()

    val queryPath = "src/test/resources/queries/query2.txt"
    val sp = new SparqlParser(queryPath)

    var intermediate = Array[Map[Node, Node]]()
    // BGP Matching
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, spark)
    assert(intermediate.length == 13)
    // Optional
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, spark)
    assert(intermediate.length == 13)
    // Optional
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, spark)
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
    val lang: Lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(nTriplePath)

    val graph = triples.asGraph()

    val queryPath = "src/test/resources/queries/query3.txt"
    val sp = new SparqlParser(queryPath)

    var intermediate = Array[Map[Node, Node]]()
    // BGP Matching
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, spark)
    assert(intermediate.length == 13)
    // Optional
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, spark)
    assert(intermediate.length == 13)
    // Optional
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, spark)
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
    val lang: Lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(nTriplePath)

    val graph = triples.asGraph()

    val queryPath = "src/test/resources/queries/query4.txt"
    val sp = new SparqlParser(queryPath)

    var intermediate = Array[Map[Node, Node]]()
    // BGP Matching
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, spark)
    assert(intermediate.length == 8)
    // Filter
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 3)
    // Union
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, spark)
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

  test("read query 5 and run the query") {

    val nTriplePath = "src/test/resources/Clustering_sampledata.nt"
    val lang: Lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(nTriplePath)

    val graph = triples.asGraph()

    val queryPath = "src/test/resources/queries/query5.txt"
    val sp = new SparqlParser(queryPath)

    var intermediate = Array[Map[Node, Node]]()
    // BGP Matching
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, spark)
    assert(intermediate.length == 6)
    // Filter
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 5)
    // Filter
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 1)
    // Select
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 1)
  }

  test("read query 6 and run the query") {

    val nTriplePath = "src/test/resources/Clustering_sampledata.nt"
    val lang: Lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(nTriplePath)

    val graph = triples.asGraph()

    val queryPath = "src/test/resources/queries/query6.txt"
    val sp = new SparqlParser(queryPath)

    var intermediate = Array[Map[Node, Node]]()
    // BGP Matching
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, spark)
    assert(intermediate.length == 3)
    // Filter
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 1)
    // Select
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 1)
  }

  test("read query 8 and run the query") {

    val nTriplePath = "src/test/resources/Clustering_sampledata.nt"
    val lang: Lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(nTriplePath)

    val graph = triples.asGraph()
    val queryPath = "src/test/resources/queries/query8.txt"
    val sp = new SparqlParser(queryPath)

    var intermediate = Array[Map[Node, Node]]()
    // BGP Matching
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, spark)
    assert(intermediate.length == 8)
    // Optional
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, spark)
    assert(intermediate.length == 8)
    // Optional
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, spark)
    assert(intermediate.length == 8)
    // Filter
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 4)
    // Order
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 4)
    // Select
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 4)
    // Limit
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 3)
  }

  test("read query 10 and run the query") {

    val nTriplePath = "src/test/resources/Clustering_sampledata.nt"
    val lang: Lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(nTriplePath)

    val graph = triples.asGraph()

    val queryPath = "src/test/resources/queries/query10.txt"
    val sp = new SparqlParser(queryPath)

    var intermediate = Array[Map[Node, Node]]()
    // BGP Matching
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, spark)
    assert(intermediate.length == 8)
    // Filter
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 6)
    // Order
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 6)
    // Select
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.head.keySet.size == 2)
    // Limit
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 5)
  }

  test("read query 11 and run the query") {

    val nTriplePath = "src/test/resources/Clustering_sampledata.nt"
    val lang: Lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(nTriplePath)

    val graph = triples.asGraph()
    val queryPath = "src/test/resources/queries/query11.txt"
    val sp = new SparqlParser(queryPath)

    var intermediate = Array[Map[Node, Node]]()
    // BGP Matching
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, spark)
    assert(intermediate.length == 5)
    // Union
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, spark)
    assert(intermediate.length == 8)
  }

  test("read query 1 with result rdd") {

    val nTriplePath = "src/test/resources/Clustering_sampledata.nt"
    val lang: Lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(nTriplePath)

    val graph = triples.asGraph()

    val queryPath = "src/test/resources/queries/query1.txt"
    val sp = new SparqlParser(queryPath)

    var intermediate = Array[Map[Node, Node]]()
    // BPG Matching
    intermediate = sp.getOps.dequeue().asInstanceOf[PatternOp].execute(intermediate, graph, spark)
    assert(intermediate.length == 6)
    // Filter
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 3)
    // Order
    intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    assert(intermediate.length == 3)
    // Select
    var interRDD = ResultFactory.create[Node](intermediate, spark)
    // interRDD.collect().foreach(println(_))
    // interRDD = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(interRDD)
    // assert(interRDD.count() == 3)
    // Distinct
    // interRDD = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(interRDD)
    // assert(interRDD.count() == 3)
    // Limit
    // intermediate = sp.getOps.dequeue().asInstanceOf[ResultOp].execute(intermediate)
    // assert(intermediate.length == 3)
  }
}
