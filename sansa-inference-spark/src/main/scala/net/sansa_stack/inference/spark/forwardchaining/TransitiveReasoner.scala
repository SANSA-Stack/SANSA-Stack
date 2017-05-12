package net.sansa_stack.inference.spark.forwardchaining

import scala.reflect.ClassTag

import org.apache.jena.graph.{Node, Triple}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

import net.sansa_stack.inference.spark.data.model.RDFGraph
import net.sansa_stack.inference.spark.data.model.TripleUtils._

/**
  * An engine to compute the transitive closure (TC) for a set of triples given in several datastructures.
  *
  * @author Lorenz Buehmann
  */
class TransitiveReasoner(sc: SparkContext, val properties: Seq[Node], val parallelism: Int)
  extends ForwardRuleReasoner {

  def this(sc: SparkContext, property: Node, parallelism: Int) {
    this(sc, Seq(property), parallelism)
  }

  def this(sc: SparkContext, parallelism: Int) {
    this(sc, Seq(), parallelism)
  }

  /**
    * Applies forward chaining to the given RDF graph and returns a new RDF graph that contains all additional
    * triples based on the underlying set of rules.
    *
    * @param graph the RDF graph
    * @return the materialized RDF graph
    */
  override def apply(graph: RDFGraph): RDFGraph = {
    if (properties.isEmpty) {
      throw new RuntimeException("A list of properties has to be given for the transitive reasoner!")
    }

    graph.triples.cache()

    // compute TC for each given property
    val tcRDDs = properties.map(p => computeTransitiveClosure(graph.triples.filter(t => t.p == p), p))

    // compute the union of all
    val triples = sc.union(tcRDDs :+ graph.triples)

    RDFGraph(triples)
  }

  def computeTransitiveClosurePairs[A, B](s: Set[(A, B)]): Set[(A, B)] = {
    val t = addTransitivePairs(s)
    // recursive call if set changed, otherwise stop and return
    if (t.size == s.size) s else computeTransitiveClosurePairs(t)
  }

  def addTransitivePairs[A, B](s: Set[(A, B)]): Set[(A, B)] = {
    s ++ (for ((x1, y1) <- s; (x2, y2) <- s if y1 == x2) yield (x1, y2))
  }

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

  private def addTransitive(triples: Set[Triple]): Set[Triple] = {
    triples ++ (
      for (t1 <- triples; t2 <- triples if t1.o == t2.s)
      yield Triple.create(t1.s, t1.p, t2.o))
  }

  /**
    * Computes the transitive closure on an RDD of triples.
    * Note, that the assumption is that all triples do have the same predicate.
    *
    * @param triples the RDD of triples
    * @return an RDD containing the transitive closure of the triples
    */
  def computeTransitiveClosure(triples: RDD[Triple]): RDD[Triple] = {
    if (triples.isEmpty()) return triples

    // get the predicate
    val predicate = triples.take(1)(0).p

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
  def computeTransitiveClosure(triples: RDD[Triple], predicate: Node): RDD[Triple] = {
    if (triples.isEmpty()) return triples
    log.info(s"computing TC for property $predicate...")

    profile {
      // we only need (s, o)
      val subjectObjectPairs = triples.map(t => (t.s, t.o)).cache()

      val tc = computeTransitiveClosure(subjectObjectPairs)

      tc.map(p => Triple.create(p._1, predicate, p._2))
    }
  }

  /**
    * Computes the transitive closure for an RDD of tuples
    *
    * @param edges the RDD of tuples
    * @return an RDD containing the transitive closure of the tuples
    */
  def computeTransitiveClosure[A: ClassTag](edges: RDD[(A, A)]): RDD[(A, A)] = {
    log.info("computing TC...")
    // we keep the transitive closure cached
    var tc = edges
    tc.cache()

    // because join() joins on keys, in addition the pairs are stored in reversed order (o, s)
    val edgesReversed = tc.map(t => (t._2, t._1))
    edgesReversed.cache()

    def f(rdd: RDD[(A, A)]): RDD[(A, A)] = {
      rdd.join(edgesReversed).map(x => (x._2._2, x._2._1))
    }

//    tc = FixpointIteration(10)(tc, f)

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
        .distinct(parallelism)
        .cache()
      nextCount = tc.count()
      i += 1
    } while (nextCount != oldCount)

    log.info("TC has " + nextCount + " edges.")
    tc
  }

  /**
    * Semi-naive computation of the transitive closure `T` for an RDD of tuples `R=(x,y)`.
    *
    * {{{
    * (1) T = R
    * (2) ∆T = R
    * (3) while ∆T != ∅ do
    * (4) ∆T = ∆T ◦ R − T
    * (5) T = T ∪ ∆T
    * (6) end
    * }}}
    *
    * @param edges the RDD of tuples `(x,y)`
    * @return an RDD containing the transitive closure of the tuples
    */
  def computeTransitiveClosureSemiNaive[A: ClassTag](edges: RDD[(A, A)]): RDD[(A, A)] = {
    log.info("computing TC...")
    // we keep the transitive closure cached
    var tc = edges
    tc.cache()

    // because join() joins on keys, in addition the pairs are stored in reversed order (o, s)
    val edgesReversed = tc.map(t => (t._2, t._1)).cache()

    var deltaTC = tc.repartition(4)

    // the join is iterated until a fixed point is reached
    var i = 1
    while(!deltaTC.isEmpty()) {
      log.info(s"iteration $i...")

      // perform the join (x, y) x (y, x), obtaining an RDD of (x=y, (y, x)) pairs,
      // then project the result to obtain the new (x, y) paths.
      deltaTC = deltaTC.join(edgesReversed)
                        .map(x => (x._2._2, x._2._1))
                        .subtract(tc).distinct().cache()

      // add to TC
      tc = tc.union(deltaTC).cache()

      i += 1
    }

    log.info("TC has " + tc.count() + " edges.")
    tc
  }

  /**
    * Computes the transitive closure for a Dataframe of triples
    *
    * @param edges the Dataframe of triples
    * @return a Dataframe containing the transitive closure of the triples
    */
  def computeTransitiveClosure(edges: Dataset[Triple]): Dataset[Triple] = {
    log.info("computing TC...")
//    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[RDFTriple]
    val spark = edges.sparkSession.sqlContext
    import spark.implicits._

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
        var joined = tc.as("A").join(tc.as("B"), $"A.o" === $"B.s").select("A.s", "A.p", "B.o").as[Triple]
//          var joined = tc
//            .join(edges, tc("o") === edges("s"))
//            .select(tc("s"), tc("p"), edges("o"))
//            .as[RDFTriple]
//        tc.sqlContext.
//          sql("SELECT A.subject, A.predicate, B.object FROM SC A INNER JOIN SC B ON A.object = B.subject")

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
