package net.sansa_stack.inference.flink.forwardchaining

import scala.reflect.ClassTag

import org.apache.flink.api.common.functions.RichJoinFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.util.Collector
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.sparql.util.NodeComparator

import net.sansa_stack.inference.flink.utils.NodeKey
import net.sansa_stack.inference.utils.Profiler

/**
  * An engine to compute the transitive closure (TC) for a set of triples given in several datastructures.
  *
  * @author Lorenz Buehmann
  */
trait TransitiveReasoner extends Profiler{

  val nodeKeyFct = (n: Node) => n.hashCode()

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
  def computeTransitiveClosure(triples: Set[Triple]): Set[Triple] = {
    val tc = addTransitive(triples)
    // recursive call if set changed, otherwise stop and return
    if (tc.size == triples.size) triples else computeTransitiveClosure(tc)
  }

  //  def addTransitive[A, B, C](s: mutable.Set[(A, B, C)]) = {
  //    s ++ (for ((s1, p1, o1) <- s; (s2, p2, o2) <- s if o1 == s2) yield (s1, p1, o2))
  //  }

  def addTransitive(triples: Set[Triple]): Set[Triple] = {
    triples ++ (for (t1 <- triples; t2 <- triples if t1.objectMatches(t2.getSubject))
      yield Triple.create(t1.getSubject, t1.getPredicate, t2.getObject))
  }

  /**
    * Computes the transitive closure on a DataSet of triples.
    * Note, that the assumption is that all triples do have the same predicate.
    *
    * @param triples the DataSet of triples
    * @return a DataSet containing the transitive closure of the triples
    */
  def computeTransitiveClosure(triples: DataSet[Triple]): DataSet[Triple] = {
    if (triples.count() == 0) return triples

    profile {
      // keep the predicate
      val predicate = triples.first(1).collect().head.getPredicate
      log.info(s"computing TC for property $predicate ...")

      // compute the TC
      var subjectObjectPairs = triples.map(t => (t.getSubject, t.getObject))

      // the join is iterated until a fixed point is reached
      var i = 1
      var oldCount = 0L
      var nextCount = triples.count()

      do {
        log.info(s"iteration $i...")
        oldCount = nextCount
        // perform the join (s1, o1) x (o2, s2), obtaining an DataSet of (s1=o2, (o1, s2)) pairs,
        // then project the result to obtain the new (s2, o1) paths.
//        import org.apache.flink.streaming.api.scala._
//        implicit val typeInfo = TypeInformation.of(classOf[(Node, Node)])
//        val newPairs = subjectObjectPairs
//          .join(subjectObjectPairs).where(_._2.hashCode()).equalTo(_._1.hashCode()) {(left, right) => (left.)}
//          .map(x => (x._1._1, x._2._2))
//          .filter(tuple => tuple._1 != tuple._2)
        implicit val typeInfo = TypeInformation.of(classOf[Int])
        subjectObjectPairs = subjectObjectPairs
          .union(
            subjectObjectPairs
              .join(subjectObjectPairs).where(_._2.hashCode()).equalTo(_._1.hashCode())(typeInfo)
            (new RichJoinFunction[(Node, Node), (Node, Node), (Node, Node)] {

              override def join(left: (Node, Node), right: (Node, Node)): (Node, Node) = (left._1, right._2)
            })
              .withForwardedFieldsFirst("_1").withForwardedFieldsSecond("_2")
//              .map(x => (x._1._1, x._2._2))
              .filter(tuple => tuple._1 != tuple._2)// omit (s1, s1)
          )
          .distinct(pair => pair._1.hashCode() * 17 + pair._2.hashCode() * 31)
        nextCount = subjectObjectPairs.count()
        i += 1
      } while (nextCount != oldCount)

      log.info("TC has " + nextCount + " triples.")
      subjectObjectPairs.map(p => Triple.create(p._1, predicate, p._2))
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
  def computeTransitiveClosureOpt(triples: DataSet[Triple]): DataSet[Triple] = {
    if (triples.count() == 0) return triples
    log.info("computing TC...")

    profile {
      // keep the predicate
      val predicate = triples.first(1).collect().head.getPredicate

      // convert to tuples needed for the JOIN operator
      val subjectObjectPairs = triples.map(t => (t.getSubject, t.getObject))

      // compute the TC
      val res = subjectObjectPairs.iterateWithTermination(10) {
        prevPaths: DataSet[(Node, Node)] =>

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
            (prev, next, out: Collector[(Node, Node)]) => {
              val prevPaths = prev.toSet
              for (n <- next)
                if (!prevPaths.contains(n)) out.collect(n)
            }
          }.withForwardedFieldsSecond("*")
          (nextPaths, terminate)
      }

      // map back to RDF triples
      res.map(p => Triple.create(p._1, predicate, p._2))
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
    * If no predicate is given, we take any triple from the dataset and use its predicate. We highly recommend to
    * provide the predicate in order to avoid unnecessary operations.
    *
    * This implementation uses the Flink iterate operator (see
    * [[https://ci.apache.org/projects/flink/flink-docs-master/dev/batch/iterations.html"]])
    *
    * @param triples the [[DataSet]] of triples
    * @param predicate the optional predicate
    * @return a [[DataSet]] containing the transitive closure of the triples
    */
  def computeTransitiveClosureOptSemiNaive(triples: DataSet[Triple], predicate: Node = null): DataSet[Triple] = {

    // if no predicate is given, we take an arbitrary triple
    // this also means, we return here if the dataset is empty (couldn't find a isEmpty() function)
    val pred = if (predicate != null) predicate else {
      val t = triples.first(1).collect()
      if(t.nonEmpty) t.head.getPredicate
      else return triples
    }


    // apparently, we have to use pairs for (subject, object) because the Jena Triple is not a Scala tuple
    // and we have to provide positions of key and value in the iterate method
    // the initial set of edges is used as input for both, the workset and the solutionset
    val initialTC = triples.map(t => (NodeKey(t.getSubject), NodeKey(t.getObject)))

    log.info("computing TC...")
    def iterate(s: DataSet[(NodeKey, NodeKey)], ws: DataSet[(NodeKey, NodeKey)])
    : (DataSet[(NodeKey, NodeKey)], DataSet[(NodeKey, NodeKey)]) = {
      val resolvedRedirects = initialTC.join(ws)
        .where(0)
        .equalTo(1)
        .map { joinResult => joinResult match {
          case (redirect, link) => (link._1, redirect._2)
        }
        }.name("TC-From-Iteration")
      (resolvedRedirects, resolvedRedirects)
    }

    val tc = initialTC
      .iterateDelta(initialTC, 10, Array(0))(iterate)
      .name("Final-TC")
    log.info("finished computing TC")
    //      .map { cl => cl}
    //      .name("Final-Redirect-Result")
    tc.map(t => Triple.create(t._1.node, pred, t._2.node))
  }

//  /**
//    * Computes the transitive closure on a DataSet of triples.
//    * Note, that the assumption is that all triples do have the same predicate.
//    * This implementation uses the Flink iterate operator (see
//    * [[https://ci.apache.org/projects/flink/flink-docs-master/dev/batch/iterations.html"]])
//    *
//    * @param triples the DataSet of triples
//    * @return a DataSet containing the transitive closure of the triples
//    */
//  def computeTransitiveClosureOptSemiNaive(triples: DataSet[Triple]): DataSet[Triple] = {
//    log.info("computing TC...")
//    def iterate(s: DataSet[Triple], ws: DataSet[Triple]): (DataSet[Triple], DataSet[Triple]) = {
//      val resolvedRedirects = triples.join(ws)
//        .where { _.getSubject }
//        .equalTo { _.getObject }
//        .map { joinResult => joinResult match {
//          case (redirect, link) =>
//            Triple.create(link.getSubject, redirect.getPredicate, redirect.getObject)
//        }
//        }.name("TC-From-Iteration")
//      (resolvedRedirects, resolvedRedirects)
//    }
//
//    val tc = triples
//      .iterateDelta(triples, 10, Array("s", "o"))(iterate)
//      .name("Final-TC")
//    log.info("finished computing TC")
//    //      .map { cl => cl}
//    //      .name("Final-Redirect-Result")
//    tc
//  }

}
