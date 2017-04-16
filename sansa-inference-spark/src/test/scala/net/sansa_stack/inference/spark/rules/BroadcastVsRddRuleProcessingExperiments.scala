package net.sansa_stack.inference.spark.rules

import org.apache.jena.vocabulary.RDFS
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.spark.data.RDFGraphLoader
import net.sansa_stack.inference.utils.{CollectionUtils, Profiler}

/**
  * @author Lorenz Buehmann
  */
object BroadcastVsRddRuleProcessingExperiments extends Profiler{

  val conf = new SparkConf()
  conf.registerKryoClasses(Array(classOf[RDFTriple]))

  // the SPARK config
  val sessionBuilder = SparkSession.builder
    .master("local[4]")
    .config("spark.eventLog.enabled", "true")
    .config("spark.hadoop.validateOutputSpecs", "false") //override output files
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.default.parallelism", "4")
    .config("spark.sql.shuffle.partitions", "4")
    .config(conf)

  var sourcePath = ""

  var session: SparkSession = null

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: BroadcastVsRddRuleProcessingExperiments <sourceFile> <targetDirectory>")
      System.exit(1)
    }

    sourcePath = args(0)

    val cnt1 = run("rdd-only", rddOnly)

    val cnt2 = run("broadcast", withBroadCast)

    println(s"rdd-only:$cnt1")
    println(s"with broadcast:$cnt2")

    val cnt3 = runIter("rdd-only-fp", rddOnly)

    val cnt4 = runIter("broadcast-fp", withBroadCast)

    println(s"rdd-only-fp:$cnt3")
    println(s"with broadcast-fp:$cnt4")

  }

  def run(name: String, f: RDD[RDFTriple] => RDD[RDFTriple]) = {
    session = sessionBuilder.appName(name).getOrCreate()

    val triples = RDFGraphLoader.loadFromFile(sourcePath, session, 4).triples
    triples.cache()

    val cnt = profile {
      f(triples).count()
    }

    session.stop()

    cnt
  }

  def runIter(name: String, f: RDD[RDFTriple] => RDD[RDFTriple]) = {
    session = sessionBuilder.appName(name).getOrCreate()

    val triples = RDFGraphLoader.loadFromFile(sourcePath, session, 4).triples
    triples.cache()

    val cnt = profile {
      fixpointIteration(f, triples).count()
    }

    session.stop()

    cnt
  }

  /*
     rdfs7	aaa rdfs:subPropertyOf bbb .
           xxx aaa yyy .                   	xxx bbb yyy .
    */

  def rddOnly(triples: RDD[RDFTriple]): RDD[RDFTriple] = {
    // extract rdfs:subPropertyOf triples
    val subPropertyOfTriples = triples.filter(t => t.p == RDFS.subPropertyOf.getURI)

    val triplesRDFS7 =
      subPropertyOfTriples.map(t => (t.s, t.o)) // (p1, p2)
      .join(
        triples.map(t => (t.p, (t.s, t.o))) // (p1, (s, o))
      ) // (p1, (p2, (s, o))
      .map(e => RDFTriple(e._2._2._1, e._2._1, e._2._2._2))

    triplesRDFS7
  }

  def withBroadCast(triples: RDD[RDFTriple]): RDD[RDFTriple] = {
    // extract rdfs:subPropertyOf triples
    val subPropertyOfTriples = triples.filter(t => t.p == RDFS.subPropertyOf.getURI)

    // a map structure should be more efficient
    val subPropertyMap = CollectionUtils.toMultiMap(subPropertyOfTriples.map(t => (t.s, t.o)).collect)

    // broadcast
    val subPropertyMapBC = session.sparkContext.broadcast(subPropertyMap)

    val triplesRDFS7 =
      triples // all triples (s p1 o)
        .filter(t => subPropertyMapBC.value.contains(t.p)) // such that p1 has a super property p2
        .flatMap(t => subPropertyMapBC.value(t.p).map(supProp => RDFTriple(t.s, supProp, t.o))) // create triple (s p2 o)

    triplesRDFS7
  }

  def fixpointIteration(f: RDD[RDFTriple] => RDD[RDFTriple], triples: RDD[RDFTriple]) = {
    var result = triples
    var iteration = 1
    var oldCount = 0L
    var nextCount = triples.count()
    do {
      println("Iteration " + iteration)
      iteration += 1
      oldCount = nextCount

      result = result.union(f(triples)).distinct().cache()

      nextCount = result.count()
    } while (nextCount != oldCount)

    result
  }
}
