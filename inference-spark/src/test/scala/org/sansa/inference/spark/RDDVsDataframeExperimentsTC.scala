package org.sansa.inference.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.sansa.inference.data.RDFTriple
import org.sansa.inference.utils.Profiler
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.util.Random

/**
  * @author Lorenz Buehmann
  */
object RDDVsDataframeExperimentsTC extends Profiler {

  val conf = new SparkConf()
  conf.registerKryoClasses(Array(classOf[RDFTriple]))

  val session = SparkSession.builder
    .appName("TC Comparison")
    .master("local[4]")
    .config("spark.eventLog.enabled", "true")
    .config("spark.hadoop.validateOutputSpecs", "false") // override output files
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.default.parallelism", "4")
    .config("spark.sql.shuffle.partitions", "4")
    .config(conf)
    .getOrCreate()
  import session.implicits._

  val numEdges = 2000
  val numVertices = 1000
  val rand = new Random(42)

  def generateGraph: Seq[(String, String)] = {
    val edges: mutable.Set[(String, String)] = mutable.Set.empty
    while (edges.size < numEdges) {
      val from = "" + rand.nextInt(numVertices)
      val to = "" + rand.nextInt(numVertices)
      if (from != to) edges.+=((from, to))
    }
    edges.toSeq
  }

  def rddBased(edges: RDD[(String, String)]) = {
    var tc = edges
    tc.cache()

    // because join() joins on keys, in addition the pairs are stored in reversed order (o, s)
    val edgesReversed = tc.map(e => (e._2, e._1))

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
        .union(tc.join(tc.map(e => (e._2, e._1))).map(x => (x._2._2, x._2._1)))
        .distinct()
        .cache()
      nextCount = tc.count()//ApproxDistinct(relativeSD = 0.05)
      println(nextCount)
      i += 1
    } while (nextCount != oldCount)

    log.info("TC has " + nextCount + " edges.")
    tc
  }

  def rddBased2(edges: RDD[(String, String)]) = {
    var tc = edges
    tc.cache()

    // because join() joins on keys, in addition the pairs are stored in reversed order (o, s)
    val edgesReversed = tc.map(e => (e._2, e._1)).cache()

    var tmp = mutable.Seq(tc)

    // the join is iterated until a fixed point is reached
    var i = 1
    var diffCnt = 0L
    do {
      log.info(s"iteration $i...")
      // perform the join (x, y) x (y, x), obtaining an RDD of (x=y, (y, x)) pairs,
      // then project the result to obtain the new (x, y) paths.
      val joined = tc.join(edgesReversed).map(x => (x._2._2, x._2._1)).distinct()
      val diff = (Seq(joined) ++ tmp).reduce(_ subtract _)

      tc = diff
        .cache()
      tmp = tmp ++ Seq(diff)

      diffCnt = tc.count()//ApproxDistinct(relativeSD = 0.05)
      println(diffCnt)
      i += 1
    } while (diffCnt > 0)

    tc = session.sparkContext.union(tmp)
    log.info("TC has " + tc.count() + " edges.")
    tc
  }

  def dataframeBased(edges: DataFrame) = {
    var tc = edges
    tc.cache()

    // the join is iterated until a fixed point is reached
    var i = 1
    var oldCount = 0L
    var nextCount = tc.count()
    do {
      log.info(s"iteration $i...")
      oldCount = nextCount

//      tc.createOrReplaceTempView("SC")
//      val joined = tc.sqlContext.sql(
//        "SELECT A.start, B.end FROM SC A INNER JOIN SC B ON A.end = B.start")

      tc =
       tc.union(
          tc.as("tc1").join(tc.as("tc2"), $"tc1.end" === $"tc2.start").select($"tc1.start", $"tc2.end")
        )
        .distinct()
        .cache()

      nextCount = tc.count()
      println(nextCount)
      i += 1
    } while (nextCount != oldCount)

//    tc.sqlContext.uncacheTable("SC")
    log.info("TC has " + nextCount + " edges.")
    tc
  }

  def main(args: Array[String]): Unit = {


    val graph = generateGraph

    val graphRDD = session.sparkContext.parallelize(graph, 2)
    val graphDataframe = graphRDD.map(e => Edge(e._1, e._2)).toDF


    profile {
      rddBased2(graphRDD)
    }
    profile {
      dataframeBased(graphDataframe)
    }


  }


  case class Edge(start: String, end: String)

}
