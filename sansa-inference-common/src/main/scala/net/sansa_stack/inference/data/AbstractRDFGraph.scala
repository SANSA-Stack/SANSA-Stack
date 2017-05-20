package net.sansa_stack.inference.data

/**
  * A data structure that comprises a collection of triples. Note, due to the implementation of the Spark
  * datastructures, this doesn't necessarily mean to be free of duplicates which is why a `distinct` operation
  * is provided.
  *
  * @author Lorenz Buehmann
  *
  */
abstract class AbstractRDFGraph[D[T], N <: RDF#Node, T <: RDF#Triple, G <: AbstractRDFGraph[D, N, T, G]](
  val triples: D[T]
) { self: G =>

  /**
    * Returns a new RDF graph that contains only triples matching the given input.
    *
    * @param s the subject
    * @param p the predicate
    * @param o the object
    * @return a new RDF graph
    */
  def find(s: Option[N] = None, p: Option[N] = None, o: Option[N] = None): G

  /**
    * Returns a new RDF graph that contains only triples matching the given input.
    *
    * @return a new RDF graph
    */
  def find(triple: T): G

  /**
    * Returns a new RDF graph that contains the union of the current RDF graph with the given RDF graph.
    *
    * @param graph the other RDF graph
    * @return the union of both RDF graphs
    */
  def union(graph: G): G

  /**
    * Returns a new RDF graph that contains the union of the current RDF graph with the given RDF graphs.
    *
    * @param graphs the other RDF graphs
    * @return the union of all RDF graphs
    */
  def unionAll(graphs: Seq[G]): G

  /**
    * Returns a new RDF graph that contains the intersection of the current RDF graph with the given RDF graph.
    *
    * @param graph the other RDF graph
    * @return the intersection of both RDF graphs
    */
  def intersection(graph: G): G

  /**
    * Returns a new RDF graph that contains the difference between the current RDF graph and the given RDF graph.
    *
    * @param graph the other RDF graph
    * @return the difference of both RDF graphs
    */
  def difference(graph: G): G

  /**
    * Returns a new RDF graph that does not contain duplicate triples.
    */
  def distinct(): G

  /**
    * Return the number of triples in the RDF graph.
    *
    * @return the number of triples in the RDF graph
    */
  def size(): Long
}
