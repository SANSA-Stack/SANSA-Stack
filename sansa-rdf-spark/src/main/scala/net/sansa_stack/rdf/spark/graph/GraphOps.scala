package net.sansa_stack.rdf.spark.graph

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import scala.Iterator
import org.apache.spark.rdd.RDD
import org.apache.jena.graph.{ Node, Triple }
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType

object GraphOps {
  @transient var spark: SparkSession = _

  /**
   * Constructs GraphX graph from RDD of triples
   * @param triples rdd of triples
   * @return object of GraphX which contains the constructed  ''graph''.
   */
  def constructGraph(triples: RDD[Triple]): Graph[Node, Node] = {
    val rs = triples.map(triple => (triple.getSubject, triple.getPredicate, triple.getObject))
    val indexedMap = (rs.map(_._1) union rs.map(_._3)).distinct.zipWithUniqueId()

    val vertices: RDD[(VertexId, Node)] = indexedMap.map(x => (x._2, x._1))
    val _nodeToId: RDD[(Node, VertexId)] = indexedMap.map(x => (x._1, x._2))

    val tuples = rs.keyBy(_._1).join(indexedMap).map({
      case (k, ((s, p, o), si)) => (o, (si, p))
    })

    val edges: RDD[Edge[Node]] = tuples.join(indexedMap).map({
      case (k, ((si, p), oi)) => Edge(si, oi, p)
    })

    org.apache.spark.graphx.Graph(vertices, edges)
  }

  /**
   * Convert a graph into a RDD of Triple.
   * @param graph GraphX graph of triples.
   * @return a RDD of triples.
   */
  def toRDD(graph: Graph[Node, Node]): RDD[Triple] = graph.triplets.map { case x => Triple.create(x.srcAttr, x.attr, x.dstAttr) }

  /**
   * Convert a graph into a DataFrame.
   * @param graph GraphX graph of triples.
   * @return a DataFrame of triples.
   */
  def toDF(graph: Graph[Node, Node]): DataFrame = {
    val schemaStructure = StructType(
      Seq(
        StructField("subject", StringType, nullable = false),
        StructField("predicate", StringType, nullable = false),
        StructField("object", StringType, nullable = false)))
    val rowRDD = toRDD(graph).map(t => Row(t.getSubject, t.getPredicate, t.getObject))
    val df = spark.createDataFrame(rowRDD, schemaStructure)
    df.createOrReplaceTempView("TRIPLES")
    df
  }

  /**
   * Convert a graph into a Dataset of Triple.
   * @param graph GraphX graph of triples.
   * @return a Dataset of triples.
   */
  def toDS(graph: Graph[Node, Node]): Dataset[Triple] = {
    implicit val encoder = Encoders.kryo[Triple]
    spark.createDataset[Triple](toRDD(graph))
  }

  /**
   * Finds triplets  of a given graph.
   * @param graph one instance of the given graph
   * @param subject
   * @param predicate
   * @param object
   * @return graph which contains subset of the reduced graph.
   */
  def find(graph: Graph[Node, Node], subject: Node, predicate: Node, `object`: Node): Graph[Node, Node] = {
    graph.subgraph({
      (triplet) =>
        triplet.srcAttr.matches(subject) && triplet.attr.matches(predicate) && triplet.dstAttr.matches(`object`)
    }, (_, _) => true)
  }

  /**
   * Compute the size of the graph
   * @param graph
   * @return the number of edges in the graph.
   */
  def size(graph: Graph[Node, Node]): Long = graph.numEdges

  /**
   * Return the union of this graph and another one.
   *
   * @param graph of the graph
   * @param other of the other graph
   * @return graph (union of all)
   */

  def union(graph: Graph[Node, Node], other: Graph[Node, Node]): Graph[Node, Node] = {
    Graph(graph.vertices.union(other.vertices.distinct()), graph.edges.union(other.edges.distinct()))
  }

  /**
   * Returns a new RDF graph that contains the intersection of the current RDF graph with the given RDF graph.
   *
   * @param graph the RDF graph
   * @param other the other RDF graph
   * @return the intersection of both RDF graphs
   */

  def intersection(graph: Graph[Node, Node], other: Graph[Node, Node]): Graph[Node, Node] = {
    Graph(graph.vertices.intersection(other.vertices.distinct()), graph.edges.intersection(other.edges.distinct()))
  }

  /**
   * Returns a new RDF graph that contains the difference between the current RDF graph and the given RDF graph.
   *
   * @param graph the RDF graph
   * @param other the other RDF graph
   * @return the difference of both RDF graphs
   */

  def difference(graph: Graph[Node, Node], other: Graph[Node, Node]): Graph[Node, Node] = {
    Graph(graph.vertices.subtract(other.vertices.distinct()), graph.edges.subtract(other.edges.distinct()))
  }

  /** Stores a map from the vertex id of a landmark to the distance to that landmark. */
  type SPMap = Map[VertexId, Int]

  private def makeMap(x: (VertexId, Int)*) = Map(x: _*)

  private def incrementMap(spmap: SPMap): SPMap = spmap.map { case (v, d) => v -> (d + 1) }

  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
    }.toMap

  /**
   * Computes shortest paths to the given set of landmark vertices.
   *
   * @tparam ED the edge attribute type (not used in the computation)
   *
   * @param graph the graph for which to compute the shortest paths
   * @param landmarks the list of landmark vertex ids. Shortest paths will be computed to each
   * landmark.
   *
   * @return a graph where each vertex attribute is a map containing the shortest-path distance to
   * each reachable landmark vertex.
   */
  def shortestPaths[VD, ED: ClassTag](graph: Graph[VD, ED], landmarks: Seq[VertexId]): Graph[SPMap, ED] = {
    val spGraph = graph.mapVertices { (vid, attr) =>
      if (landmarks.contains(vid)) makeMap(vid -> 0) else makeMap()
    }

    val initialMessage = makeMap()

    def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
      addMaps(attr, msg)
    }

    def sendMessage(edge: EdgeTriplet[SPMap, _]): Iterator[(VertexId, SPMap)] = {
      val newAttr = incrementMap(edge.dstAttr)
      if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
      else Iterator.empty
    }

    Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addMaps)
  }

}