package org.dissect.rdf.spark.model

import com.hp.hpl.jena.graph.{ Node => JNode }
import org.apache.spark.rdd.RDD

/**
 * Functions for RDD of Triples
 */
class TripleRDD(rdd: RDD[Triple]) {

  def subjects: RDD[JNode] = rdd.map(_.s)

  def predicates: RDD[JNode] = rdd.map(_.p)

  def objects: RDD[JNode] = rdd.map(_.o)

  def mapSubjects(f: (JNode) => JNode): RDD[Triple] = {
    rdd.map{
      case Triple(s, p, o) => Triple(f(s), p, o)
    }
  }

  def mapPredicates(f: (JNode) => JNode): RDD[Triple] = {
    rdd.map{
      case Triple(s, p, o) => Triple(s, f(p), o)
    }
  }

  def mapObjects(f: (JNode) => JNode): RDD[Triple] = {
    rdd.map{
      case Triple(s, p, o) => Triple(s, p, f(o))
    }
  }

  def filterSubjects(f: (JNode) => Boolean): RDD[Triple] = {
    rdd.filter{
      case Triple(s, p, o) => f(s)
    }
  }

  def filterPredicates(f: (JNode) => Boolean): RDD[Triple] = {
    rdd.filter{
      case Triple(s, p, o) => f(p)
    }
  }

  def filterObjects(f: (JNode) => Boolean): RDD[Triple] = {
    rdd.filter{
      case Triple(s, p, o) => f(o)
    }
  }

  def mapNodes(f: (JNode) => JNode): RDD[Triple] = {
    rdd.map{
      case Triple(s, p, o) => Triple(f(s), f(p), f(o))
    }
  }
}

object TripleRDD {
  implicit def tripleFunctions(rdd: RDD[Triple]): TripleRDD = new TripleRDD(rdd)
}
