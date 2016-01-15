package org.dissect.rdf.spark.model
import com.hp.hpl.jena.graph.{ Node => rddNode }
import com.hp.hpl.jena.graph.{ Node_Literal => rddLiteral }
import com.hp.hpl.jena.rdf
import com.hp.hpl.jena.vocabulary.RDF
import org.apache.spark.rdd.RDD

object TripleModel {

  type Triple = (rddNode, rddNode, rddNode)

  def getLiterals(rdd: RDD[Triple]) = rdd.map(_._3).flatMap {
    case l: Literal => Some(l);
    case _ => None
  }

  def getTypes(rdd: RDD[Triple]) = rdd.flatMap {
    case (s, p, o) if RDF.predicate.equals(p) => Some(o);
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

  def isIRI(n: rddNode) = n.isURI()

  def isBlank(n: rddNode) = n.isBlank()

  def isLiteral(n: rddNode) = n.isLiteral()

  def getTriple() = Triple

}