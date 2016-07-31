package org.sansa.inference.flink.forwardchaining

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.util.Collector
import org.sansa.inference.data.RDFTriple
import org.sansa.inference.flink.data.RDFGraph
import org.sansa.inference.utils.Profiler

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * A forward chaining based reasoner.
  *
  * @author Lorenz Buehmann
  */
trait ForwardRuleReasoner extends Profiler{

  /**
    * Applies forward chaining to the given RDF graph and returns a new RDF graph that contains all additional
    * triples based on the underlying set of rules.
    *
    * @param graph the RDF graph
    * @return the materialized RDF graph
    */
  def apply(graph: RDFGraph) : RDFGraph

//  def computeTransitiveClosure[A, B, C](s: mutable.Set[(A, B, C)]): mutable.Set[(A, B, C)] = {
//    val t = addTransitive(s)
//    // recursive call if set changed, otherwise stop and return
//    if (t.size == s.size) s else computeTransitiveClosure(t)
//  }

  def computeTransitiveClosure(s: mutable.Set[RDFTriple]): mutable.Set[RDFTriple] = {
    val t = addTransitive(s)
    // recursive call if set changed, otherwise stop and return
    if (t.size == s.size) s else computeTransitiveClosure(t)
  }

//  def addTransitive[A, B, C](s: mutable.Set[(A, B, C)]) = {
//    s ++ (for ((s1, p1, o1) <- s; (s2, p2, o2) <- s if o1 == s2) yield (s1, p1, o2))
//  }

  def addTransitive(s: mutable.Set[RDFTriple]) = {
    s ++ (for (t1 <- s; t2 <- s if t1.`object` == t2.subject) yield RDFTriple(t1.subject, t1.predicate, t2.`object`))
  }

  def computeTransitiveClosure(triples: DataSet[RDFTriple]): DataSet[RDFTriple] = {
    if(triples.count() == 0) return triples
    log.info("computing TC...")

    profile {
      // keep the predicate
      val predicate = triples.first(1).collect().head.predicate

      // compute the TC
      var subjectObjectPairs = triples.map(t => (t.subject, t.`object`))

      // because join() joins on keys, in addition the pairs are stored in reversed order (o, s)
      val objectSubjectPairs = subjectObjectPairs.map(t => (t._2, t._1))

      // the join is iterated until a fixed point is reached
      var i = 1
      var oldCount = 0L
      var nextCount = triples.count()
      do {
        log.info(s"iteration $i...")
        oldCount = nextCount
        // perform the join (s1, o1) x (o2, s2), obtaining an DataSet of (s1=o2, (o1, s2)) pairs,
        // then project the result to obtain the new (s2, o1) paths.
        subjectObjectPairs = subjectObjectPairs
          .union(
            subjectObjectPairs
              .join(objectSubjectPairs).where(0).equalTo(0)
              .map(x => (x._2._2, x._1._2))
              .filter(tuple => tuple._1 != tuple._2)// omit (s1, s1)
          )
          .distinct()
        nextCount = subjectObjectPairs.count()
        i += 1
      } while (nextCount != oldCount)

      println("TC has " + nextCount + " triples.")
      subjectObjectPairs.map(p => RDFTriple(p._1, predicate, p._2))
    }
  }

  def computeTransitiveClosure2(triples: DataSet[RDFTriple]): DataSet[RDFTriple] = {
    if(triples.count() == 0) return triples
    log.info("computing TC...")

    profile {
      // keep the predicate
      val predicate = triples.first(1).collect().head.predicate

      // convert to tuples needed for the JOIN operator
      val subjectObjectPairs = triples.map(t => (t.subject, t.`object`))

      // compute the TC
      val res = subjectObjectPairs.iterateWithTermination(10) {
        prevPaths: DataSet[(String, String)] =>

          val nextPaths = prevPaths
            .join(subjectObjectPairs).where(1).equalTo(0) {
            (left, right) => (left._1, right._2)
            }
            .union(prevPaths)
            .groupBy(0, 1)
            .reduce((l ,r) => l)

          val terminate = prevPaths
            .coGroup(nextPaths)
            .where(0).equalTo(0) {
            (prev, next, out: Collector[(String, String)]) => {
              val prevPaths = prev.toSet
              for (n <- next)
                if (!prevPaths.contains(n)) out.collect(n)
            }
          }.withForwardedFieldsSecond("*")
          (nextPaths, terminate)
      }

      // map back to RDF triples
      res.map(p => RDFTriple(p._1, predicate, p._2))
    }
  }

  def computeTransitiveClosure[A: ClassTag: TypeInformation](edges: DataSet[(A, A)]): DataSet[(A, A)] = {
    log.info("computing TC...")
    // we keep the transitive closure cached
    var tc = edges

    // because join() joins on keys, in addition the pairs are stored in reversed order (o, s)
    val edgesReversed = tc.map(t => (t._2, t._1))

    // the join is iterated until a fixed point is reached
    var i = 1
    var oldCount = 0L
    var nextCount = tc.count()
    do {
      log.info(s"iteration $i...")
      oldCount = nextCount
      // perform the join (x, y) x (y, x), obtaining an DataSet of (x=y, (y, x)) pairs,
      // then project the result to obtain the new (x, y) paths.
      val join = tc.join(edgesReversed).where(0).equalTo(0)
      join.print()
      tc = tc
        .union(join.map(x => (x._2._2, x._2._1)))
        .distinct()
      nextCount = tc.count()
      i += 1
    } while (nextCount != oldCount)

    println("TC has " + nextCount + " edges.")
    tc
  }

  /**
    * Extracts all triples for the given predicate.
    *
    * @param triples the triples
    * @param predicate the predicate
    * @return the set of triples that contain the predicate
    */
  def extractTriples(triples: mutable.Set[RDFTriple], predicate: String): mutable.Set[RDFTriple] = {
    triples.filter(triple => triple.predicate == predicate)
  }

  /**
    * Extracts all triples for the given predicate.
    *
    * @param triples the DataSet of triples
    * @param predicate the predicate
    * @return the DataSet of triples that contain the predicate
    */
  def extractTriples(triples: DataSet[RDFTriple], predicate: String): DataSet[RDFTriple] = {
    triples.filter(triple => triple.predicate == predicate)
  }

  /**
    * Extracts all triples that match the given subject, predicate and object if defined.
    *
    * @param triples the DataSet of triples
    * @param subject the subject
    * @param predicate the predicate
    * @param obj the object
    * @return the DataSet of triples that match
    */
  def extractTriples(triples: DataSet[RDFTriple],
                     subject: Option[String],
                     predicate: Option[String],
                     obj: Option[String]): DataSet[RDFTriple] = {
    var extractedTriples = triples

    if(subject.isDefined) {
      extractedTriples = extractedTriples.filter(triple => triple.subject == subject.get)
    }

    if(predicate.isDefined) {
      extractedTriples = extractedTriples.filter(triple => triple.predicate == predicate.get)
    }

    if(obj.isDefined) {
      extractedTriples = extractedTriples.filter(triple => triple.`object` == obj.get)
    }

    extractedTriples
  }

}
