package net.sansa_stack.rdf.spark.userstories

import net.sansa_stack.rdf.spark.utils.Logging
import org.apache.spark._
import scala.collection.mutable
import net.sansa_stack.rdf.spark.model.JenaSparkRDDOps

/**
 * Created by Gezim Sejdiu on 23/09/2016.
 * Driver program for running user stories.
 */
object UserStories extends Logging {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        "Usage: UserStories <Story> <file>  [User Story]")
      System.err.println("Supported 'Story' as follows:")
      System.err.println("  triplecount    Compute Triple Count")
      System.err.println("  uc             Class usage count")
      System.err.println("  cs             Count all subjects")
      System.err.println("  cp             Count all predicates")
      System.err.println("  co             Count all objects")
      System.exit(1)
    }
    val Story = args(0)
    val fname = args(1)
    val optionsList = args.drop(2).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _             => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }
    val options = mutable.Map(optionsList: _*)
    val conf = new SparkConf()

    Story match {
      case "triplecount" =>

        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("======================================")
        println("|             TripleCount            |")
        println("======================================")

        val sparkContext = new SparkContext(conf.setAppName("TripleCount(" + fname + ")")
          .setMaster("local[*]")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator"))

        val ops = JenaSparkRDDOps(sparkContext)
        import ops._

        val triples = fromNTriples(fname, "http://dbpedia.org").toSeq

        val triplesRDD = sparkContext.parallelize(triples)

        val TripleCount = triplesRDD.filter(f =>
          f.getPredicate.isURI()).distinct()

        println("TripleCount : " + TripleCount)

        sparkContext.stop()
      case "uc" =>
        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("======================================")
        println("|      Class usage count             |")
        println("======================================")

        val sparkContext = new SparkContext(conf.setAppName("ClassUsage(" + fname + ")")
          .setMaster("local[*]")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator"))

        val ops = JenaSparkRDDOps(sparkContext)
        import ops._

        val triples = fromNTriples(fname, "http://dbpedia.org").toSeq

        val triplesRDD = sparkContext.parallelize(triples)

        val usedclasses = triplesRDD
          .filter(f => f.getPredicate.equals("http://www.w3.org/2000/01/rdf-schema#Class") && f.getObject.isURI())
          .map(obj => (obj, 1))
          .reduceByKey(_ + _)

        println(usedclasses.collect().mkString(", "))

        sparkContext.stop()
      case _ =>
        println("Invalid task type.")
    }

  }

}