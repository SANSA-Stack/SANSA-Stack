package net.sansa_stack.rdf.spark.model

import org.apache.jena.graph
import org.apache.jena.graph.{Node, Node_URI, Node_Concrete}
import org.apache.spark.rdd.RDD

/**
 * Implicit wrapper for functions for RDD of Jena Triples.
 *
 * @author Nilesh Chakraborty <nilesh@nileshc.com>, Gezim Sejdiu <g.sejdiu@gmail.com>
 */
class TripleRDD(@transient graphRDD: JenaSparkRDD#Graph) extends Serializable with JenaSparkRDDOps {
  @transient override def sparkContext = graphRDD.sparkContext

  def getTriples: Iterable[JenaSparkRDD#Triple] =
    getTriples(graphRDD)

  def getSubjects: RDD[JenaSparkRDD#Node] =
    graphRDD.map(_.getSubject)

  def getPredicates: RDD[JenaSparkRDD#URI] =
    graphRDD.map(_.getPredicate.asInstanceOf[Node_URI])

  def getObjects: RDD[JenaSparkRDD#Node] =
    graphRDD.map(_.getObject)

  def getSubjectsWithPredicate(predicate: JenaSparkRDD#URI): RDD[JenaSparkRDD#Node] =
    getSubjectsRDD(graphRDD, predicate)

  def getSubjectsWithPredicate(predicate: JenaSparkRDD#URI, objectt: JenaSparkRDD#Node): RDD[JenaSparkRDD#Node] =
    getSubjectsRDD(graphRDD, predicate, objectt)

  def getObjectsWithPredicate(predicate: JenaSparkRDD#URI): RDD[JenaSparkRDD#Node] =
    getObjectsRDD(graphRDD, predicate)

  def getObjectsWithPredicate(subject: JenaSparkRDD#Node, predicate: JenaSparkRDD#URI): RDD[JenaSparkRDD#Node] =
    getObjectsRDD(graphRDD, subject, predicate)

  def mapSubjects(func: (JenaSparkRDD#Node) => JenaSparkRDD#Node): JenaSparkRDD#Graph = {
    graphRDD.map {
      case Triple(s, p, o) => Triple[JenaSparkRDD](func(s), p, o)
      // forcing type argument to JenaSparkRDD because wrong type is automatically inferred. TODO: see if this needs fixing during unit testing
    }
  }

  def mapPredicates(func: (JenaSparkRDD#URI) => JenaSparkRDD#URI): JenaSparkRDD#Graph = {
    graphRDD.map {
      case Triple(s, p, o) => Triple(s, func(p), o)
    }
  }

  def mapObjects(func: (JenaSparkRDD#Node) => JenaSparkRDD#Node): JenaSparkRDD#Graph = {
    graphRDD.map {
      case Triple(s, p, o) => Triple(s, p, func(o))
    }
  }

  def filterSubjects(func: (JenaSparkRDD#Node) => Boolean): JenaSparkRDD#Graph = {
    graphRDD.filter {
      case Triple(s, p, o) => func(s)
    }
  }

  def filterPredicates(func: (JenaSparkRDD#URI) => Boolean): JenaSparkRDD#Graph = {
    graphRDD.filter {
      case Triple(s, p, o) => func(p)
    }
  }

  def filterObjects(func: (JenaSparkRDD#Node) => Boolean): JenaSparkRDD#Graph = {
    graphRDD.filter {
      case Triple(s, p, o) => func(o)
    }
  }

  def mapURIs(func: (JenaSparkRDD#URI) => JenaSparkRDD#URI): JenaSparkRDD#Graph = {
    def mapper(n: JenaSparkRDD#Node): JenaSparkRDD#Node = foldNode(n)(func, bnode => bnode, lit => lit)
    graphRDD.map {
      case Triple(s, p, o) => Triple[JenaSparkRDD](mapper(s), mapper(p).asInstanceOf[JenaSparkRDD#URI], mapper(s))
      // forcing type argument to JenaSparkRDD because wrong type is automatically inferred. TODO: see if this needs fixing during unit testing
    }
  }

  def mapLiterals(func: (JenaSparkRDD#Literal) => JenaSparkRDD#Literal): JenaSparkRDD#Graph = {
    def mapper(n: JenaSparkRDD#Node): JenaSparkRDD#Node = foldNode(n)(uri => uri, bnode => bnode, func)
    graphRDD.map {
      case Triple(s, p, o) => Triple[JenaSparkRDD](mapper(s), p, mapper(o))
      // forcing type argument to JenaSparkRDD because wrong type is automatically inferred. TODO: see if this needs fixing during unit testing
    }
  }

  def find(subject: JenaSparkRDD#NodeMatch, predicate: JenaSparkRDD#NodeMatch, objectt: JenaSparkRDD#NodeMatch): JenaSparkRDD#Graph =
    findGraph(graphRDD, subject, predicate, objectt)

  def saveGraphToNTriples(file: String): Unit = saveGraphToNTriples(graphRDD, file)

  def saveGraphToSequenceFile(file: String): Unit = saveGraphToSequenceFile(graphRDD, file)
}


object TripleRDD {
  implicit def tripleFunctions(rdd: RDD[JenaSparkRDD#Triple]): TripleRDD = new TripleRDD(rdd)

}
