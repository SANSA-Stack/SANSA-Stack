package org.dissect.rdf.spark.model

import org.apache.spark.rdd.RDD

/**
 * Implicit wrapper for functions for RDD of Jena Triples.
 *
 * @author Nilesh Chakraborty <nilesh@nileshc.com>, Gezim Sejdiu <g.sejdiu@gmail.com>
 */
class TripleRDD(graphRDD: JenaSpark#Graph) extends JenaNodeOps[JenaSpark] with SparkGraphOps[JenaSpark] {
  val sparkContext = graphRDD.sparkContext

  def getTriples: Iterable[JenaSpark#Triple] =
    getTriples(graphRDD)

  def getSubjects: RDD[JenaSpark#Node] =
    graphRDD.map(_.getSubject)

  def getPredicates: RDD[JenaSpark#Node] =
    graphRDD.map(_.getPredicate)

  def getObjects: RDD[JenaSpark#Node] =
    graphRDD.map(_.getObject)

  def getSubjectsWithPredicate(predicate: JenaSpark#URI): RDD[JenaSpark#Node] =
    getSubjectsRDD(graphRDD, predicate)

  def getSubjectsWithPredicate(predicate: JenaSpark#URI, objectt: JenaSpark#Node): RDD[JenaSpark#Node] =
    getSubjectsRDD(graphRDD, predicate, objectt)

  def getObjectsWithPredicate(predicate: JenaSpark#URI): RDD[JenaSpark#Node] =
    getObjectsRDD(graphRDD, predicate)

  def getObjectsWithPredicate(subject: JenaSpark#Node, predicate: JenaSpark#URI): RDD[JenaSpark#Node] =
    getObjectsRDD(graphRDD, subject, predicate)

  def mapSubjects(func: (JenaSpark#Node) => JenaSpark#Node): JenaSpark#Graph = {
    graphRDD.map {
      case Triple(s, p, o) => Triple(func(s), p, o)
    }
  }

  def mapPredicates(func: (JenaSpark#URI) => JenaSpark#URI): JenaSpark#Graph = {
    graphRDD.map {
      case Triple(s, p, o) => Triple(s, func(p), o)
    }
  }

  def mapObjects(func: (JenaSpark#Node) => JenaSpark#Node): JenaSpark#Graph = {
    graphRDD.map {
      case Triple(s, p, o) => Triple(s, p, func(o))
    }
  }

  def filterSubjects(func: (JenaSpark#Node) => Boolean): JenaSpark#Graph = {
    graphRDD.filter {
      case Triple(s, p, o) => func(s)
    }
  }

  def filterPredicates(func: (JenaSpark#URI) => Boolean): JenaSpark#Graph = {
    graphRDD.filter {
      case Triple(s, p, o) => func(p)
    }
  }

  def filterObjects(func: (JenaSpark#Node) => Boolean): JenaSpark#Graph = {
    graphRDD.filter {
      case Triple(s, p, o) => func(o)
    }
  }

  def mapURIs(func: (JenaSpark#URI) => JenaSpark#URI): JenaSpark#Graph = {
    def mapper(n: JenaSpark#Node) = foldNode(n)(func, bnode => bnode, lit => lit)
    graphRDD.map {
      case Triple(s, p, o) => Triple(mapper(s), mapper(p).asInstanceOf[JenaSpark#URI], mapper(s))
    }
  }

  def mapLiterals(func: (JenaSpark#Literal) => JenaSpark#Literal): JenaSpark#Graph = {
    def mapper(n: JenaSpark#Node) = foldNode(n)(uri => uri, bnode => bnode, func)
    graphRDD.map {
      case Triple(s, p, o) => Triple(mapper(s), p, mapper(o))
    }
  }

  def find(subject: JenaSpark#NodeMatch, predicate: JenaSpark#NodeMatch, objectt: JenaSpark#NodeMatch): JenaSpark#Graph =
    findGraph(graphRDD, subject, predicate, objectt)
}


object TripleRDD {
  implicit def tripleFunctions(rdd: RDD[JenaSpark#Triple]): TripleRDD = new TripleRDD(rdd)
}