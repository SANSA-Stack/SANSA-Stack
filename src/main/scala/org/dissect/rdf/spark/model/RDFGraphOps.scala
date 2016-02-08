package org.dissect.rdf.spark.model

/**
 * @author Nilesh Chakraborty <nilesh@nileshc.com>
 */
trait RDFGraphOps[Rdf <: RDF]
  extends URIOps[Rdf]
  with RDFDSL[Rdf] { this: RDFNodeOps[Rdf] =>
  // graph

  def makeGraph(it: Iterable[Rdf#Triple]): Rdf#Graph

  def getTriples(graph: Rdf#Graph): Iterable[Rdf#Triple]

  def graphSize(g: Rdf#Graph): Int

  // graph traversal

  def findGraph(graph: Rdf#Graph, subject: Rdf#NodeMatch, predicate: Rdf#NodeMatch, objectt: Rdf#NodeMatch): Rdf#Graph

  def find(graph: Rdf#Graph, subject: Rdf#NodeMatch, predicate: Rdf#NodeMatch, objectt: Rdf#NodeMatch): Iterator[Rdf#Triple]

  def getObjects(graph: Rdf#Graph, subject: Rdf#Node, predicate: Rdf#URI): Iterable[Rdf#Node] =
    find(graph, toConcreteNodeMatch(subject), toConcreteNodeMatch(predicate), ANY).map(t => fromTriple(t)._3).toIterable

  def getPredicates(graph: Rdf#Graph, subject: Rdf#Node): Iterable[Rdf#URI] =
    find(graph, toConcreteNodeMatch(subject), ANY, ANY).map(t => fromTriple(t)._2).toIterable

  def getSubjects(graph: Rdf#Graph, predicate: Rdf#URI, obj: Rdf#Node): Iterable[Rdf#Node] =
    find(graph, ANY, toConcreteNodeMatch(predicate), toConcreteNodeMatch(obj)).map(t => fromTriple(t)._1).toIterable

  // graph operations

  def union(graphs: Seq[Rdf#Graph]): Rdf#Graph

  def intersection(graphs: Seq[Rdf#Graph]): Rdf#Graph

  def difference(g1: Rdf#Graph, g2: Rdf#Graph): Rdf#Graph

  def isomorphism(left: Rdf#Graph, right: Rdf#Graph): Boolean
}

object RDFGraphOps {
  def apply[Rdf <: RDF](implicit ops: RDFGraphOps[Rdf]): RDFGraphOps[Rdf] = ops
}



