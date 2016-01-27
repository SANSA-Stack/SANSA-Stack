package org.dissect.rdf.spark.model

import com.hp.hpl.jena.graph.{ Node => JNode }
import com.hp.hpl.jena.graph.{ Node_Literal => JLiteral }
import com.hp.hpl.jena.graph.{ Node_URI => JURI }
import org.apache.spark.rdd.RDD
import org.dissect.rdf.spark.utils.Logging

/**
 * Functions for RDD of Triples
 */
class TripleRDD(rdd: RDD[Triple]) extends Serializable with Logging {

  def subjects: RDD[JNode] = rdd.map(_.s)

  def predicates: RDD[JNode] = rdd.map(_.p)

  def objects: RDD[JNode] = rdd.map(_.o)

  def mapSubjects(f: (JNode) => JNode): RDD[Triple] = {
    rdd.map {
      case Triple(s, p, o) => Triple(f(s), p, o)
    }
  }

  def mapPredicates(f: (JNode) => JNode): RDD[Triple] = {
    rdd.map {
      case Triple(s, p, o) => Triple(s, f(p), o)
    }
  }

  def mapObjects(f: (JNode) => JNode): RDD[Triple] = {
    rdd.map {
      case Triple(s, p, o) => Triple(s, p, f(o))
    }
  }

  def filterSubjects(f: (JNode) => Boolean): RDD[Triple] = {
    rdd.filter {
      case Triple(s, p, o) => f(s)
    }
  }

  def filterPredicates(f: (JNode) => Boolean): RDD[Triple] = {
    rdd.filter {
      case Triple(s, p, o) => f(p)
    }
  }

  def filterObjects(f: (JNode) => Boolean): RDD[Triple] = {
    rdd.filter {
      case Triple(s, p, o) => f(o)
    }
  }

  def mapNodes(f: (JNode) => JNode): RDD[Triple] = {
    rdd.map {
      case Triple(s, p, o) => Triple(f(s), f(p), f(o))
    }
  }

  /*
   * mapLiterals apply user defined function to all literals
   * @f the triple set
   */
  def mapLiterals(f: (JNode) => JNode): RDD[Triple] = {
    rdd.map {
      case Triple(s, p, o: JLiteral) => Triple(s, p, f(o))
    }
  }

  /*
   * Apply user defined functions to all RUIs restricted to subject part of a triple
   */
  def mapSubjectsURI(f: (JNode) => JNode): RDD[Triple] = {
    rdd.map {
      case Triple(s: JURI, p, o) => Triple(f(s), p, o)
    }
  }

  /*
   * Apply user defined functions to all RUIs restricted to predicate part of a triple
   */
  def mapPredicateURI(f: (JNode) => JNode): RDD[Triple] = {
    rdd.map {
      case Triple(s, p: JURI, o) => Triple(s, f(p), o)
    }
  }

  /*
   * Apply user defined functions to all RUIs restricted to object part of a triple
   */
  def mapObjectURI(f: (JNode) => JNode): RDD[Triple] = {
    rdd.map {
      case Triple(s, p, o: JURI) => Triple(s, p, f(o))
    }
  }

  /*
   * Apply user defined function to all URIs
   */
  def mapURIs(f: (JNode) => JNode): RDD[Triple] = {
    rdd.map {
      case Triple(s: JURI, p: JURI, o: JURI) => Triple(f(s), f(p), f(o))
    }
  }

}

object TripleRDD {
  implicit def tripleFunctions(rdd: RDD[Triple]): TripleRDD = new TripleRDD(rdd)
}
