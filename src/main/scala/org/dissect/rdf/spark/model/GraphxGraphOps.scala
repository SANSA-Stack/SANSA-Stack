package org.dissect.rdf.spark.model

import org.apache.spark.SparkContext
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{Graph => SparkGraph, EdgeTriplet, VertexId, Edge}

import scala.util.hashing.MurmurHash3

/**
 * Spark/GraphX based implementation of RDFGraphOps
 *
 * @author Nilesh Chakraborty <nilesh@nileshc.com>
 */
trait GraphxGraphOps[Rdf <: SparkGraphX]
  extends RDFGraphOps[Rdf]
  with URIOps[Rdf]
  with RDFDSL[Rdf] { this: RDFNodeOps[Rdf] =>

  protected val sparkContext: SparkContext

  // graph

  protected def makeGraph(triples: Iterable[Rdf#Triple]): Rdf#Graph = {
    val triplesRDD = sparkContext.parallelize(triples.toSeq)
    makeGraph(triplesRDD)
  }

  protected def makeGraph(triples: RDD[Rdf#Triple]): Rdf#Graph = {
    val spo: RDD[(Rdf#Node, (Rdf#URI, Rdf#Node))] = triples.map {
      case Triple(s, p, o) => (s, (p, o))
    }

    val vertexIDs = spo.flatMap {
      case (s: Rdf#Node, (p: Rdf#URI, o: Rdf#Node)) =>
        Seq(s, p.asInstanceOf[Rdf#Node], o)
    }.zipWithUniqueId()

    val vertices: RDD[(VertexId, Rdf#Node)] = vertexIDs.map(v => (v._2, v._1))

    val subjectMappedEdges = spo.join(vertexIDs).map {
      case (s: Rdf#Node, ((p: Rdf#URI, o: Rdf#Node), sid: Long)) =>
        (o, (sid, p))
    }

    val subjectObjectMappedEdges: RDD[Edge[Rdf#URI]] = subjectMappedEdges.join(vertexIDs).map {
      case (o: Rdf#Node, ((sid: Long, p: Rdf#URI), oid: Long)) =>
        Edge(sid, oid, p)
    }

    val subjectVertexIds: RDD[(VertexId, Option)] = subjectObjectMappedEdges.map(x => (x.srcId, None))
    val objectVertexIds: RDD[(VertexId, Option)] = subjectObjectMappedEdges.map(x => (x.dstId, None))

    SparkGraph(vertices, subjectObjectMappedEdges)
  }

  protected def makeHashedVertexGraph(triples: RDD[Rdf#Triple]): Rdf#Graph = {
    val spo: RDD[(Rdf#Node, Rdf#URI, Rdf#Node)] = triples.map {
      case Triple(s, p, o) => (s, p, o)
    }

    def hash(s: Rdf#Node) = MurmurHash3.stringHash(s.toString).toLong

    val vertices: RDD[(VertexId, Rdf#Node)] = spo.flatMap {
      case (s: Rdf#Node, p: Rdf#URI, o: Rdf#Node) =>
        Seq((hash(s), s), (hash(p), p), (hash(o), o))
    }

    val edges: RDD[Edge[Rdf#URI]] = spo.map {
      case (s: Rdf#Node, p: Rdf#URI, o: Rdf#Node) =>
        Edge(hash(s), hash(o), p)
    }

    SparkGraph(vertices, edges)
  }

  protected def getTriples(graph: Rdf#Graph): Iterable[Rdf#Triple] =
    graph.triplets.map(x => Triple(x.srcAttr, x.attr, x.dstAttr)).toLocalIterator.toIterable

  // graph traversal

  protected def getObjectsRDD(graph: Rdf#Graph, subject: Rdf#Node, predicate: Rdf#URI): RDD[Rdf#Node] =
    findGraph(graph, toConcreteNodeMatch(subject), toConcreteNodeMatch(predicate), ANY).triplets.map(_.dstAttr)

  protected def getObjectsRDD(graph: Rdf#Graph, predicate: Rdf#URI): RDD[Rdf#Node] =
    findGraph(graph, ANY, toConcreteNodeMatch(predicate), ANY).triplets.map(_.dstAttr)

  protected def getPredicatesRDD(graph: Rdf#Graph, subject: Rdf#Node): RDD[Rdf#URI] =
    findGraph(graph, toConcreteNodeMatch(subject), ANY, ANY).triplets.map(_.attr)

  protected def getSubjectsRDD(graph: Rdf#Graph, predicate: Rdf#URI, obj: Rdf#Node): RDD[Rdf#Node] =
    findGraph(graph, ANY, toConcreteNodeMatch(predicate), toConcreteNodeMatch(obj)).triplets.map(_.srcAttr)

  protected def getSubjectsRDD(graph: Rdf#Graph, predicate: Rdf#URI): RDD[Rdf#Node] =
    findGraph(graph, ANY, toConcreteNodeMatch(predicate), ANY).triplets.map(_.srcAttr)

  // graph traversal

  protected def findGraph(graph: Rdf#Graph, subject: Rdf#NodeMatch, predicate: Rdf#NodeMatch, objectt: Rdf#NodeMatch): Rdf#Graph = {
    graph.subgraph({
      (triplet: EdgeTriplet[Rdf#Node, Rdf#URI]) =>
        matchNode(triplet.srcAttr, subject) && matchNode(triplet.attr, predicate) && matchNode(triplet.dstAttr, objectt)
    }, (_, _) => true)
  }

  protected def find(graph: Rdf#Graph, subject: Rdf#NodeMatch, predicate: Rdf#NodeMatch, objectt: Rdf#NodeMatch): Iterator[Rdf#Triple] =
    findGraph(graph, subject, predicate, objectt).triplets.map(x => Triple(x.srcAttr, x.attr, x.dstAttr)).toLocalIterator

  // graph operations

  protected def union(graphs: Seq[Rdf#Graph]): Rdf#Graph =
    graphs.reduce {
      case (left: Rdf#Graph, right: Rdf#Graph) =>
        val newGraph = SparkGraph(left.vertices.union(right.vertices), left.edges.union(right.edges))
        newGraph.partitionBy(RandomVertexCut).groupEdges( (attr1, attr2) => attr1 )
    }

  protected def intersection(graphs: Seq[Rdf#Graph]): Rdf#Graph =
    graphs.reduce {
      case (left: Rdf#Graph, right: Rdf#Graph) =>
        val newGraph = SparkGraph(left.vertices.intersection(right.vertices), left.edges.intersection(right.edges))
        newGraph.partitionBy(RandomVertexCut).groupEdges( (attr1, attr2) => attr1 )
    }

  protected def difference(g1: Rdf#Graph, g2: Rdf#Graph): Rdf#Graph = {
    /// subtract triples; edge triplet intersection is collected into memory - is there a better way? Joining somehow?
    val matchingTriplets = g1.triplets.intersection(g2.triplets).collect().toSet
    g1.subgraph({
      (triplet: EdgeTriplet[Rdf#Node, Rdf#URI]) =>
        matchingTriplets.contains(triplet)
    }, (_, _) => true)
  }

  /**
   * Implement Spark algorithm for determining whether left and right are isomorphic
   */
  protected def isomorphism(left: Rdf#Graph, right: Rdf#Graph): Boolean = ???

  protected def graphSize(g: Rdf#Graph): Long = g.numEdges
}
