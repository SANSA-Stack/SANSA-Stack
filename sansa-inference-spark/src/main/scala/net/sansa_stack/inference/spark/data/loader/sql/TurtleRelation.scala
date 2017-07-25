package net.sansa_stack.inference.spark.data.loader.sql

import java.io.ByteArrayInputStream

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import scala.util.{Failure, Success, Try}

/**
  * A custom relation that represents RDF triples loaded from files in Turtle syntax.
  *
  * @param location
  * @param userSchema
  * @param sqlContext
  */
class TurtleRelation(location: String, userSchema: StructType)
                    (@transient val sqlContext: SQLContext)
    extends BaseRelation
      with TableScan
      with PrunedScan
      with Serializable {

    override def schema: StructType = {
      if (this.userSchema != null) {
        this.userSchema
      }
      else {
        StructType(
          Seq(
            StructField("s", StringType, true),
            StructField("p", StringType, true),
            StructField("o", StringType, true)
        ))
      }
    }


  import scala.collection.JavaConverters._

    override def buildScan(): RDD[Row] = {

      val confHadoop = new org.apache.hadoop.mapreduce.Job().getConfiguration
      confHadoop.set("textinputformat.record.delimiter", ".\n")

      // 1. parse the Turtle file into an RDD[String] with each entry containing a full Turtle snippet
      val turtleRDD = sqlContext.sparkContext.newAPIHadoopFile(
        location, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], confHadoop)
        .filter(!_._2.toString.trim.isEmpty)
        .map{ case (_, v) => v.toString }

//      turtleRDD.collect().foreach(chunk => println("Chunk" + chunk))

      // 2. we need the prefixes - two options:
      // a) assume that all prefixes occur in the beginning of the document
      // b) filter all lines that contain the prefixes
      val prefixes = turtleRDD.filter(_.startsWith("@prefix"))

      // we broadcast the prefixes
      val prefixesBC = sqlContext.sparkContext.broadcast(prefixes.collect())

      // use the Jena Turtle parser to get the triples
      val rows = turtleRDD.flatMap(ttl => {
        cleanly(new ByteArrayInputStream((prefixesBC.value.mkString("\n") + ttl).getBytes))(_.close()) { is =>
          // parse the text snippet with Jena
          val iter = RDFDataMgr.createIteratorTriples(is, Lang.TURTLE, null).asScala

          iter.map(t => Row.fromTuple((t.getSubject.toString, t.getPredicate.toString, t.getObject.toString))).toSeq
        }.get

      })

      rows
    }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val confHadoop = new org.apache.hadoop.mapreduce.Job().getConfiguration
    confHadoop.set("textinputformat.record.delimiter", ".\n")

    // 1. parse the Turtle file into an RDD[String] with each entry containing a full Turtle snippet
    val turtleRDD = sqlContext.sparkContext.newAPIHadoopFile(
      location, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], confHadoop)
      .filter(!_._2.toString.trim.isEmpty)
      .map{ case (_, v) => v.toString.trim }

//    turtleRDD.collect().foreach(chunk => println("Chunk:" + chunk))

    // 2. we need the prefixes - two options:
    // a) assume that all prefixes occur in the beginning of the document
    // b) filter all lines that contain the prefixes
    val prefixes = turtleRDD.filter(_.startsWith("@prefix"))

    // we broadcast the prefixes
    val prefixesBC = sqlContext.sparkContext.broadcast(prefixes.collect())

    // use the Jena Turtle parser to get the triples
    val rows = turtleRDD.flatMap(ttl => {
//      println("snippet:" + prefixesBC.value.mkString("\n") + ttl)
      cleanly(new ByteArrayInputStream((prefixesBC.value.mkString("\n") + ttl).getBytes))(_.close()) { is =>
        // parse the text snippet with Jena
        val iter = RDFDataMgr.createIteratorTriples(is, Lang.TURTLE, null).asScala

        iter.map(t => Row.fromTuple((t.getSubject.toString, t.getPredicate.toString, t.getObject.toString))).toSeq
      }.get

    })

    rows
  }


  def cleanly[A, B](resource: A)(cleanup: A => Unit)(doWork: A => B): Try[B] = {
    try {
      Success(doWork(resource))
    } catch {
      case e: Exception => Failure(e)
    }
    finally {
      try {
        if (resource != null) {
          cleanup(resource)
        }
      } catch {
        case e: Exception => println(e) // should be logged
      }
    }
  }
}