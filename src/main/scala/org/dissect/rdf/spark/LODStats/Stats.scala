package org.dissect.rdf.spark.LODStats

import org.apache.spark.graphx._
import org.apache.spark.rdd._
import scala.collection.mutable.{ HashMap, HashSet }

/*
 * RDFStat
 * 
 */
class RDFStats(graph: Graph[String, String], triples: RDD[(String, String, String)]) extends Serializable {

  //triples from graph

  //  val subjects: RDD[String] = graph.triplets.map(triplet => triplet.srcAttr)
  //  val predicates: RDD[String] = graph.triplets.map(triplet => triplet.attr)
  //  val objects: RDD[String] = graph.triplets.map(triplet => triplet.dstAttr)

  val subjects: RDD[String] = triples.map(triple => triple._1)
  val objects: RDD[String] = triples.map(triple => triple._3)
  val predicate: RDD[String] = triples.map(triple => triple._2)

  val statsList: HashSet[String] = new HashSet[String]

  var count: Long = 0

  /*
   * 1.
   * Used Classes Criterion.
   * Output format: {'classname': usage count}
   */
  def UsedClasses(): Long = {
    val uc = triples.map(tr => (tr._2, tr._3))
      .filter(f =>
        f._1.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") & (isIRI(f._2)))
      .count
    uc
  }

  /*
   * Distinct number of entities.
   * Entity - triple, where ?s is IRI (not blank)
   */
  def BlanksAsObject(): Long = {
    val c = objects.filter(f => f.startsWith("_:")).count
    count += c
    c
    //graph.unpersist(true)
  }
  /*
 * Distinct number of entities.
 * Entity - triple, where ?s is IRI (not blank)
 */
  def BlanksAsSubject(): Long = {
    //graph.edges.filter(f => f.attr.startsWith("313f9908:")).count
    val c = subjects.filter(f => f.contains(":-")).count
    count += c
    c
  }

  /*
   * Gather hierarchy of classes seen.
   */
  def ClassHierarchyDepth() {

  }

  /*
   * Used Classes Criterion.
   * Output format: {'classname': usage count}
   */
  def ClassesDefined() {

  }

  /*
   * Histogram of types used for literals.
   */
  def Datatypes() {

  }

  /*
   * Distinct number of entities.
   * Entity - triple, where ?s is IRI
   *  (not blank)
   */
  def DistinctObjects() {
  }

  /*
   *  Distinct number of entities.
   *  Entity - triple, where ?s is IRI (not blank)
   */
  def Entities() {

  }

  /*
   * Number of labeled subjects.
   */
  def LabeledSubjects() {

  }

  /*
   * Number of triples with owl#sameAs as predicate
   */
  def SameAs(): Long = {
    val c = subjects.filter(f =>
      f.equals("http://www.w3.org/2002/07/owl#sameAs")).count
    count += c
    c
  }

  /*
   * Page rank of a graph.
   */
  def PageRank(tol: Double): VertexRDD[Double] = {
    val pagerank = graph.pageRank(tol).vertices
    pagerank
  }

  def printPageRanks(vertices: RDD[(VertexId, String)], top: Int) {
    val report = PageRank(0.00001).join(vertices)
      .map({ case (k, (r, v)) => (r, v, k) })
      .sortBy(top - _._1)
    report.take(top).foreach(println)
  }

  /*
   * total number of triples
   */
  def totalTriples(): Long = {
    val c = triples.count
    c
  }

  /*
   * Nodes that are unique
   */
  def uniqueNodes(): Long = {
    val distinctNodes: RDD[String] = (subjects union objects).distinct;
    distinctNodes.count
  }

  /*
   * 
   */
  def VoID(void_model: String, dataset: String) {

  }

  /*
   * Avarage length of untyped/string literals.
   */
  def StringLength() {

  }

  def runStats() {
    val bAso = BlanksAsObject
    val bAsS = BlanksAsSubject
    val uc = UsedClasses
    println("Stats [bAsocount:" + bAso + ", bAsScount:" + bAsS + ", uccount:" + uc + "]")

  }

  def isIRI(node: String): Boolean = {
    var res = false;
    if (node.startsWith("http://")) //isURI())
      res = true
    res
  }

}

object RDFStats {
  def apply(graph: Graph[String, String], triples: RDD[(String, String, String)]) = new RDFStats(graph, triples)

}

