package net.sansa_stack.inference.spark.forwardchaining

import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.utils.Profiler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.reflect.ClassTag

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

  private def addTransitive(triples: Set[RDFTriple]): Set[RDFTriple] = {
    triples ++ (for (t1 <- triples; t2 <- triples if t1.`object` == t2.subject) yield RDFTriple(t1.subject, t1.predicate, t2.`object`))
  }

  /**
    * Computes the transitive closure on an RDD of triples.
    * Note, that the assumption is that all triples do have the same predicate.
    *
    * @param triples the RDD of triples
    * @return an RDD containing the transitive closure of the triples
    */
  def computeTransitiveClosure(triples: RDD[RDFTriple]): RDD[RDFTriple] = {
    // get the predicate
    val predicate = triples.take(1)(0).predicate

    // compute TC
    computeTransitiveClosure(triples, predicate)
  }

  /**
    * Computes the transitive closure for the given predicate on an RDD of triples.
    *
    * @param triples the RDD of triples
    * @param predicate the predicate
    * @return an RDD containing the transitive closure of the triples
    */
  def computeTransitiveClosure(triples: RDD[RDFTriple], predicate: String): RDD[RDFTriple] = {
    if(triples.isEmpty()) return triples
    log.info(s"computing TC for property $predicate...")

    profile {
      // compute the TC
      var subjectObjectPairs = triples.map(t => (t.subject, t.`object`)).cache()

      // because join() joins on keys, in addition the pairs are stored in reversed order (o, s)
      val objectSubjectPairs = subjectObjectPairs.map(t => (t._2, t._1))

      // the join is iterated until a fixed point is reached
      var i = 1
      var oldCount = 0L
      var nextCount = triples.count()
      do {
        log.info(s"iteration $i...")
        oldCount = nextCount
        // perform the join (s1, o1) x (o2, s2), obtaining an RDD of (s1=o2, (o1, s2)) pairs,
        // then project the result to obtain the new (s2, o1) paths.
        subjectObjectPairs = subjectObjectPairs
          .union(subjectObjectPairs.join(objectSubjectPairs).map(x => (x._2._2, x._2._1)))
          .filter(tuple => tuple._1 != tuple._2) // omit (s1, s1)
          .distinct()
          .cache()
        nextCount = subjectObjectPairs.count()
        i += 1
      } while (nextCount != oldCount)

      log.info(s"TC for $predicate has " + nextCount + " triples.")
      subjectObjectPairs.map(p => new RDFTriple(p._1, predicate, p._2))
    }
  }

  /**
    * Computes the transitive closure for an RDD of tuples
    *
    * @param edges the RDD of triples
    * @return an RDD containing the transitive closure of the tuples
    */
  def computeTransitiveClosure[A:ClassTag](edges: RDD[(A, A)]): RDD[(A, A)] = {
    log.info("computing TC...")
    // we keep the transitive closure cached
    var tc = edges
    tc.cache()

    // because join() joins on keys, in addition the pairs are stored in reversed order (o, s)
    val edgesReversed = tc.map(t => (t._2, t._1))

    // the join is iterated until a fixed point is reached
    var i = 1
    var oldCount = 0L
    var nextCount = tc.count()
    do {
      log.info(s"iteration $i...")
      oldCount = nextCount
      // perform the join (x, y) x (y, x), obtaining an RDD of (x=y, (y, x)) pairs,
      // then project the result to obtain the new (x, y) paths.
      tc = tc
        .union(tc.join(edgesReversed).map(x => (x._2._2, x._2._1)))
        .distinct()
        .cache()
      nextCount = tc.count()
      i += 1
    } while (nextCount != oldCount)

    println("TC has " + nextCount + " edges.")
    tc
  }

  /**
    * Computes the transitive closure for a Dataframe of triples
    *
    * @param edges the Dataframe of triples
    * @return a Dataframe containing the transitive closure of the triples
    */
  def computeTransitiveClosure[A:ClassTag](edges: DataFrame): DataFrame = {
    log.info("computing TC...")

    profile {
      // we keep the transitive closure cached
      var tc = edges
      tc.cache()

      // the join is iterated until a fixed point is reached
      var i = 1
      var oldCount = 0L
      var nextCount = tc.count()
      do {
        log.info(s"iteration $i...")
        oldCount = nextCount

        //        val df1 = tc.alias("df1")
        //        val df2 = tc.alias("df2")
        // perform the join (x, y) x (y, x), obtaining an RDD of (x=y, (y, x)) pairs,
        // then project the result to obtain the new (x, y) paths.

        tc.createOrReplaceTempView("SC")
        var joined = tc.sqlContext.sql("SELECT A.subject, A.predicate, B.object FROM SC A INNER JOIN SC B ON A.object = B.subject")

        //      joined.explain()
        //      var joined = df1.join(df2, df1("object") === df2("subject"), "inner")
        //      println("JOINED:\n" + joined.collect().mkString("\n"))
        //      joined = joined.select(df2(s"df1.$col1"), df1(s"df1.$col2"))
        //      println(joined.collect().mkString("\n"))

        tc = tc
          .union(joined)
          .distinct()
          .cache()
        nextCount = tc.count()
        i += 1
      } while (nextCount != oldCount)

      tc.sqlContext.uncacheTable("SC")
      log.info("TC has " + nextCount + " edges.")
      tc
    }
  }

}
