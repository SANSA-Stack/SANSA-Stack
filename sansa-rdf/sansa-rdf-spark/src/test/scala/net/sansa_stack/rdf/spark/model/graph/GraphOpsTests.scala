package net.sansa_stack.rdf.spark.model.graph

import java.nio.file.Files

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.graph.{Node, Triple}

import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite

class GraphOpsTests extends AnyFunSuite with DataFrameSuiteBase {

  import net.sansa_stack.rdf.spark.model._

  var triples: RDD[Triple] = _
  var graph: Graph[Node, Node] = _
  val numTriples = 10 // technically it should be 9 but Jena compares datatypes by object identity and serialization creates different objects

  override def conf: SparkConf = {
    val conf = super.conf
    conf
      .set("spark.master", "local[1]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")
    conf
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val path = getClass.getResource("/loader/data.nt").getPath
    val lang: Lang = Lang.NTRIPLES

    triples = spark.rdf(lang)(path).cache()
    graph = triples.asGraph().cache()
  }

  test("loading N-Triples file into Graph should match") {
    val size = graph.size()

    assert(size == numTriples)
  }

  test("conversation of Graph into RDD should result match") {
    val graph2rdd = graph.toRDD()

    val cnt = graph2rdd.count()
    assert(cnt == numTriples)
  }

  test("getting the triples of Graph should match") {
    val graph2rdd = graph.getTriples()

    val cnt = graph2rdd.count()
    assert(cnt == numTriples)
  }

  test("getting the subjects/predicates/objects of Graph should match") {
    val subjects = graph.getSubjects().distinct()
    val subjectsCnt = subjects.count()
    assert(subjectsCnt == 6)

    val predicates = graph.getPredicates().distinct()
    val predicatesCnt = predicates.count()
    assert(predicatesCnt == 7)

    val objects = graph.getObjects().distinct()
    val objectsCnt = objects.count()
    assert(objectsCnt == 9)

    val cnt = (subjects union predicates union objects).distinct().count()
    assert(cnt == 20)
  }

  test("filtering the subjects as URI of the Graph should match") {
    val subjectsAsURI = graph.filterSubjects(_.isURI())

    val cnt = subjectsAsURI.size()
    assert(cnt == 8)
  }

  test("filtering the predicate which are Variable on the graph should match") {
    val predicatesAsVariable = graph.filterPredicates(_.isVariable())

    val cnt = predicatesAsVariable.size()
    assert(cnt == 0)
  }

  test("filtering the objects which are literals on the graph should match") {
    val objectsAsLiterals = graph.filterObjects(_.isLiteral())

    val cnt = objectsAsLiterals.size()
    assert(cnt == 7)
  }

  test("converting graph to Json should match") {
    // create temp dir
    val outputDir = Files.createTempDirectory("sansa-graph")
    outputDir.toFile.deleteOnExit()

    val output = outputDir.toString + "/data.json"
    graph.saveGraphToJson(output)

    assert(true)

  }
}
