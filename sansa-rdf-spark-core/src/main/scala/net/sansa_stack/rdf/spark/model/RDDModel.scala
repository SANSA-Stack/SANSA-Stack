package net.sansa_stack.rdf.spark.model

import org.apache.jena.graph.{ Node => rddNode }
import org.apache.jena.graph.{ Node_Literal => rddLiteral }
import org.apache.jena.graph.{ Node_Literal => rddLiteral }
import org.apache.jena.rdf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.MapPartitionsRDD
import org.apache.jena.vocabulary.{ RDF => JenaRDF }
import org.apache.jena.rdf.model.Literal
import org.apache.jena.graph.Node

object RDDModel extends Serializable {

  type Triple = (rddNode, rddNode, rddNode)

  def getLiterals(rdd: RDD[Triple]) = rdd.map(_._3).flatMap {
    case l: Literal => Some(l);
    case _ => None
  }
  def getTypes(rdd: RDD[Triple]) = rdd.flatMap {
    case (s, p, o) if JenaRDF.predicate.equals(p) => Some(o);
    case _ => None
  }

  /*
   * mapLiterals apply user defined function to all literals
   * @rdd the triple set
   */
  def mapLiterals(rdd: RDD[Triple]) = rdd.map(_._3).flatMap {
    case l: rddLiteral => Some(l);
    case _ => None
  }

  def mapSubjectsURI(rdd: RDD[Triple]) = rdd.map(_._1).flatMap {
    case subj: Node => Some(subj)
    case _ => None
  }

  /*
   * Filter function by subject
   * @param rdd the triple set
   * @param subj the subject node
   * @return matched subject nodes
   */
  def filterSubjects(rdd: RDD[Triple], subj: rddNode) = {
    rdd.filter(f => f._1.equals(subj)).map(_._1)
  }

  /*
   * Filter function by predicate
   * @param rdd the triple set
   * @param subj the subject
   * @return matched predicates nodes
   */
  def filterPredicates(rdd: RDD[Triple], pred: rddNode) = {
    rdd.filter(f => f._2.equals(pred)).map(_._2)
  }

  /*
   * Filter function by objects
   * @param rdd the triple set
   * @param subj the subject
   * @return matched predicates nodes
   */
  def filterObjects(rdd: RDD[Triple], obj: rddNode) = {
    rdd.filter(f => f._3.equals(obj)).map(_._3)
  }

  /*
  def filter(f: Triple=> Boolean) = {
    new MapPartitionsRDD[Triple, Triple](
      this,
      (iter) => iter.filter(f))
  }
  *
  */

  /*
   * Merge two RDTS
   */
  def mergeRDDs(srcRDD: RDD[Triple], dscRDD: RDD[Triple]) = {
    //rdds.map(_.iterator(split, context)).reduce(_ ++ _)
    srcRDD.union(dscRDD)
  }

  def isIRI(n: rddNode) = n.isURI()

  def isBlank(n: rddNode) = n.isBlank()

  def isLiteral(n: rddNode) = n.isLiteral()

  def getTriple() = Triple

}