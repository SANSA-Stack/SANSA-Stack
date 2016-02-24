package org.dissect.rdf.spark.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Spark based implementation of RDFGraphOps
 *
 * @author Nilesh Chakraborty <nilesh@nileshc.com>
 */
trait SparkRDDGraphOps[Rdf <: SparkRDD]
  extends RDFGraphOps[Rdf]
  with URIOps[Rdf]
  with RDFDSL[Rdf] { this: RDFNodeOps[Rdf] =>

  protected val sparkContext: SparkContext

  // graph

  protected def makeGraph(triples: Iterable[Rdf#Triple]): Rdf#Graph =
    sparkContext.parallelize(triples.toSeq)

  protected def getTriples(graph: Rdf#Graph): Iterable[Rdf#Triple] =
    graph.toLocalIterator.toIterable

  // graph traversal

  protected def getObjectsRDD(graph: Rdf#Graph, subject: Rdf#Node, predicate: Rdf#URI): RDD[Rdf#Node] =
    findGraph(graph, toConcreteNodeMatch(subject), toConcreteNodeMatch(predicate), ANY).map(t => fromTriple(t)._3)

  protected def getObjectsRDD(graph: Rdf#Graph, predicate: Rdf#URI): RDD[Rdf#Node] =
    findGraph(graph, ANY, toConcreteNodeMatch(predicate), ANY).map(t => fromTriple(t)._3)

  protected def getPredicatesRDD(graph: Rdf#Graph, subject: Rdf#Node): RDD[Rdf#URI] =
    findGraph(graph, toConcreteNodeMatch(subject), ANY, ANY).map(t => fromTriple(t)._2)

  protected def getSubjectsRDD(graph: Rdf#Graph, predicate: Rdf#URI, obj: Rdf#Node): RDD[Rdf#Node] =
    findGraph(graph, ANY, toConcreteNodeMatch(predicate), toConcreteNodeMatch(obj)).map(t => fromTriple(t)._1)

  protected def getSubjectsRDD(graph: Rdf#Graph, predicate: Rdf#URI): RDD[Rdf#Node] =
    findGraph(graph, ANY, toConcreteNodeMatch(predicate), ANY).map(t => fromTriple(t)._1)

  // graph traversal

  protected def findGraph(graph: Rdf#Graph, subject: Rdf#NodeMatch, predicate: Rdf#NodeMatch, objectt: Rdf#NodeMatch): Rdf#Graph = {
    graph.filter {
      case Triple(s, p, o) =>
        matchNode(s, subject) && matchNode(s, subject) && matchNode(s, subject)
    }
  }

  protected def find(graph: Rdf#Graph, subject: Rdf#NodeMatch, predicate: Rdf#NodeMatch, objectt: Rdf#NodeMatch): Iterator[Rdf#Triple] =
    findGraph(graph, subject, predicate, objectt).toLocalIterator

  // graph operations

  protected def union(graphs: Seq[Rdf#Graph]): Rdf#Graph =
    graphs.reduce(_ union _)

  protected def intersection(graphs: Seq[Rdf#Graph]): Rdf#Graph =
    graphs.reduce(_ intersection _)

  protected def difference(g1: Rdf#Graph, g2: Rdf#Graph): Rdf#Graph =
    g1 subtract g2

  /**
   * Implement Spark algorithm for determining whether left and right are isomorphic
   */
  protected def isomorphism(left: Rdf#Graph, right: Rdf#Graph): Boolean = ???

  protected def graphSize(g: Rdf#Graph): Long = g.count()
}
