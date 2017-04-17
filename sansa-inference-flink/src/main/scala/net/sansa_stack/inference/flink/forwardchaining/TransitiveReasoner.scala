package net.sansa_stack.inference.flink.forwardchaining

import scala.reflect.ClassTag

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.util.Collector

import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.utils.Profiler

/**
  * An engine to compute the transitive closure (TC) for a set of triples given in several datastructures.
  *
  * @author Lorenz Buehmann
  */
trait TransitiveReasoner extends Profiler{

  //  def computeTransitiveClosure[A, B, C](s: mutable.Set[(A, B, C)]): mutable.Set[(A, B, C)] = {
  //    val t = addTransitive(s)
  //    // recursive call if set changed, otherwise stop and return
  //    if (t.size == s.size) s else computeTransitiveClosure(t)
  //  }

  /**
    * Computes the transitive closure on a set of triples, i.e. it is computed in-memory by the driver.
    * Note, that the assumption is that all triples do have the same predicate.
    *
    * @param triples the set of triples
    * @return a set containing the transitive closure of the triples
    */
  def computeTransitiveClosure(triples: Set[RDFTriple]): Set[RDFTriple] = {
    val tc = addTransitive(triples)
    // recursive call if set changed, otherwise stop and return
    if (tc.size == triples.size) triples else computeTransitiveClosure(tc)
  }

  //  def addTransitive[A, B, C](s: mutable.Set[(A, B, C)]) = {
  //    s ++ (for ((s1, p1, o1) <- s; (s2, p2, o2) <- s if o1 == s2) yield (s1, p1, o2))
  //  }

  def addTransitive(triples: Set[RDFTriple]): Set[RDFTriple] = {
    triples ++ (for (t1 <- triples; t2 <- triples if t1.o == t2.s) yield RDFTriple(t1.s, t1.p, t2.o))
  }

  /**
    * Computes the transitive closure on a DataSet of triples.
    * Note, that the assumption is that all triples do have the same predicate.
    *
    * @param triples the DataSet of triples
    * @return a DataSet containing the transitive closure of the triples
    */
  def computeTransitiveClosure(triples: DataSet[RDFTriple]): DataSet[RDFTriple] = {
    if (triples.count() == 0) return triples
    log.info("computing TC...")

    profile {
      // keep the predicate
      val predicate = triples.first(1).collect().head.p

      // compute the TC
      var subjectObjectPairs = triples.map(t => (t.s, t.o))

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

      log.info("TC has " + nextCount + " triples.")
      subjectObjectPairs.map(p => RDFTriple(p._1, predicate, p._2))
    }
  }

  /**
    * Computes the transitive closure on a DataSet of triples.
    * Note, that the assumption is that all triples do have the same predicate.
    * This implementation uses the Flink iterate operator (see
    * [[https://ci.apache.org/projects/flink/flink-docs-master/dev/batch/iterations.html"]])
    *
    * @param triples the DataSet of triples
    * @return a DataSet containing the transitive closure of the triples
    */
  def computeTransitiveClosureOpt(triples: DataSet[RDFTriple]): DataSet[RDFTriple] = {
    if (triples.count() == 0) return triples
    log.info("computing TC...")

    profile {
      // keep the predicate
      val predicate = triples.first(1).collect().head.p

      // convert to tuples needed for the JOIN operator
      val subjectObjectPairs = triples.map(t => (t.s, t.o))

      // compute the TC
      val res = subjectObjectPairs.iterateWithTermination(10) {
        prevPaths: DataSet[(String, String)] =>

          val nextPaths = prevPaths
            .join(subjectObjectPairs).where(1).equalTo(0) {
            (left, right) => (left._1, right._2)
          }
            .union(prevPaths)
            .groupBy(0, 1)
            .reduce((l, r) => l)

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

  /**
    * Computes the transitive closure for a DataSet of tuples
    *
    * @param edges the DataSet of tuples
    * @return a tuples containing the transitive closure of the tuples
    */
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

    log.info("TC has " + nextCount + " edges.")
    tc
  }

  /**
    * Computes the transitive closure on a DataSet of triples.
    * Note, that the assumption is that all triples do have the same predicate.
    * This implementation uses the Flink iterate operator (see
    * [[https://ci.apache.org/projects/flink/flink-docs-master/dev/batch/iterations.html"]])
    *
    * @param triples the DataSet of triples
    * @return a DataSet containing the transitive closure of the triples
    */
  def computeTransitiveClosureOptSemiNaive(triples: DataSet[RDFTriple]): DataSet[RDFTriple] = {
    log.info("computing TC...")
    def iterate(s: DataSet[RDFTriple], ws: DataSet[RDFTriple]): (DataSet[RDFTriple], DataSet[RDFTriple]) = {
      val resolvedRedirects = triples.join(ws)
        .where { _.s }
        .equalTo { _.o }
        .map { joinResult => joinResult match {
          case (redirect, link) =>
            RDFTriple(link.s, redirect.p, redirect.o)
        }
        }.name("TC-From-Iteration")
      (resolvedRedirects, resolvedRedirects)
    }

    val tc = triples
      .iterateDelta(triples, 10, Array("s", "o"))(iterate)
      .name("Final-TC")
    log.info("finished computing TC")
    //      .map { cl => cl}
    //      .name("Final-Redirect-Result")
    tc
  }



}
