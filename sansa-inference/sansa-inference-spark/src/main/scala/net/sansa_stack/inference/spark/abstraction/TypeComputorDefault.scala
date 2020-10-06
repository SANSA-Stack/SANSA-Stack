package net.sansa_stack.inference.spark.abstraction
import java.io.File
import java.net.URI

import scala.collection.mutable

import org.apache.jena.vocabulary.RDF
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import net.sansa_stack.inference.spark.data.loader.RDFGraphLoader

import net.sansa_stack.inference.spark.data.model.TripleUtils._
import org.apache.jena.graph.{Node, Triple}

/**
  * @author Lorenz Buehmann
  */
class TypeComputorDefault extends TypeComputor {

  override def computeTypes(aboxTriples: RDD[Triple]): RDD[((Set[Node], Set[Node], Set[Node]), Iterable[Node])] = {

    val ind2OutgoingTriples = aboxTriples
      .map(t => (t.s, (true, t.p, t.o)))
//      .groupByKey()

//    val properties2Individuals = ind2OutgoingTriples
//      .map(e => {
//        val ind = e._1
//
//        val types = e._2.filter(pair => pair._1 == RDF.`type`.getURI).map(pair => pair._2).toSet
//        val properties = e._2.map(pair => pair._1).toSet
//
//        ((types, properties), ind)
//      })
//      .groupByKey()

    val ind2IncomingTriples = aboxTriples
      .filter(t => t.p != RDF.`type`.asNode())
      .map(t => (t.o, (false, t.p, t.s)))
//      .groupByKey()

    val ind2Triples = ind2OutgoingTriples.union(ind2IncomingTriples).groupByKey()

    val properties2Individuals = ind2Triples
      .map(e => {
        val ind = e._1

        val types = e._2.filter(pair => pair._2 == RDF.`type`.asNode()).map(pair => pair._3).toSet

        val propertiesIn = e._2.filter(entry => !entry._1).map(entry => entry._2).toSet
        val propertiesOut = e._2.filter(entry => entry._1).map(entry => entry._2).toSet

        ((types, propertiesOut, propertiesIn), ind)
      })
      .groupByKey()

    properties2Individuals

//    val ind2IncomingTriples =
//      aboxTriples.map(t => (t.o, (t.p, t.s))).groupByKey()
//
//    val subjects = aboxTriples.map(t => t.subject)
//
//    val concepts = aboxTriples.map(t => (t.s, t.o)).groupByKey()
  }

}

object TypeComputorDefault {
  def main(args: Array[String]): Unit = {
    // set the parallelism
    val parallelism = 4

    // the SPARK config
    val session = SparkSession.builder
      .appName("Abstract Graph generator")
      .master("local[4]")
      .config("spark.eventLog.enabled", "true")
      .config("spark.hadoop.validateOutputSpecs", "false") // override output files
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.default.parallelism", parallelism)
      .config("spark.ui.showConsoleProgress", "false")
      .getOrCreate()

    val graph = RDFGraphLoader.loadFromDisk(session, Seq(URI.create(args(0))), parallelism)

    val typeComputor = new TypeComputorDefault

    val triples = graph.triples
      .filter(t => t.p != RDF.`type`.asNode() || t.o.getURI.startsWith("http://swat.cse.lehigh.edu/onto/univ-bench.owl#"))
      .filter(t => t.p == RDF.`type`.asNode() || t.p.getURI.startsWith("http://swat.cse.lehigh.edu/onto/univ-bench.owl#"))

//    triples.cache()

//    println(triples.count())

    val types = typeComputor.computeTypes(triples)

    println(types.collect().map(e => (e._1, e._2.size)).mkString("\n"))
  }
}
