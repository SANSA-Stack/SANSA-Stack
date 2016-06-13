package org.dissect.rdf.spark.model

import java.io.{StringReader, BufferedReader}

import org.apache.jena.graph.Triple
import org.apache.jena.riot.RDFDataMgr
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import org.apache.jena.riot.Lang
import RDFDSL._
import org.apache.jena.graph.{Triple => JenaTriple}
import scala.reflect.ClassTag

/**
 * Spark based implementation of RDFGraphOps
 *
 * @author Nilesh Chakraborty <nilesh@nileshc.com>
 */
trait SparkRDDGraphOps[Rdf <: SparkRDD{ type Blah = Rdf }]
  extends RDFGraphOps[Rdf] { this: RDFNodeOps[Rdf] =>

  @transient protected def sparkContext: SparkContext

  // graph
  def loadGraphFromNTriples(file: String, baseIRI: String): Rdf#Graph =
    sparkContext.textFile(file).mapPartitions {
      kryoWrap {
        case it =>
          val triples = it.mkString("\n")
          fromNTriples(triples, baseIRI).iterator
      }
    }

  def saveGraphToNTriples(graph: Rdf#Graph, file: String): Unit = {
    graph.mapPartitions {
      kryoWrap {
        case it =>
          toNTriples(it.toIterable).split("\n").iterator
      }
    }.saveAsTextFile(file)
  }

  // TODO: Do sequenceFile I/O using Avro, more efficient
  def loadGraphFromSequenceFile(file: String): Rdf#Graph =
    sparkContext.objectFile(file)

  // TODO: Do sequenceFile I/O using Avro, more efficient
  def saveGraphToSequenceFile(graph:Rdf#Graph, file: String): Unit =
    graph.saveAsObjectFile(file)

  def makeGraph(triples: Iterable[Rdf#Triple]): Rdf#Graph =
    sparkContext.parallelize(triples.toSeq)

  def getTriples(graph: Rdf#Graph): Iterable[Rdf#Triple] =
    graph.toLocalIterator.toIterable

  // graph traversal

  def getObjectsRDD(graph: Rdf#Graph, subject: Rdf#Node, predicate: Rdf#URI): RDD[Rdf#Node] =
    findGraph(graph, toConcreteNodeMatch(subject), toConcreteNodeMatch(predicate), ANY).map(t => fromTriple(t)._3)

  def getObjectsRDD(graph: Rdf#Graph, predicate: Rdf#URI): RDD[Rdf#Node] =
    findGraph(graph, ANY, toConcreteNodeMatch(predicate), ANY).map(t => fromTriple(t)._3)

  def getPredicatesRDD(graph: Rdf#Graph, subject: Rdf#Node): RDD[Rdf#URI] =
    findGraph(graph, toConcreteNodeMatch(subject), ANY, ANY).map(t => fromTriple(t)._2)

  def getSubjectsRDD(graph: Rdf#Graph, predicate: Rdf#URI, obj: Rdf#Node): RDD[Rdf#Node] =
    findGraph(graph, ANY, toConcreteNodeMatch(predicate), toConcreteNodeMatch(obj)).map(t => fromTriple(t)._1)

  def getSubjectsRDD(graph: Rdf#Graph, predicate: Rdf#URI): RDD[Rdf#Node] =
    findGraph(graph, ANY, toConcreteNodeMatch(predicate), ANY).map(t => fromTriple(t)._1)

  // graph traversal

  def findGraph(graph: Rdf#Graph, subject: Rdf#NodeMatch, predicate: Rdf#NodeMatch, objectt: Rdf#NodeMatch): Rdf#Graph = {
    graph.filter {
      kryoWrap {
        // FIXME: Ugly code, supposed to work with Triple(s, p, o) directly but causing MatchError in some cases, no pun intended
        case x =>
          Triple.unapply(x.asInstanceOf[Rdf#Triple]) match {
            case Some((s, p, o)) =>
              val result = matchNode(s, subject) && matchNode(p, predicate) && matchNode(o, objectt)
              result
          }
      }
    }
  }

  def find(graph: Rdf#Graph, subject: Rdf#NodeMatch, predicate: Rdf#NodeMatch, objectt: Rdf#NodeMatch): Iterator[Rdf#Triple] =
    findGraph(graph, subject, predicate, objectt).toLocalIterator

  // graph operations

  def union(graphs: Seq[Rdf#Graph]): Rdf#Graph =
    graphs.reduce(_ union _)

  def intersection(graphs: Seq[Rdf#Graph]): Rdf#Graph =
    graphs.reduce(_ intersection _)

  def difference(g1: Rdf#Graph, g2: Rdf#Graph): Rdf#Graph =
    g1 subtract g2

  /**
   * Implement Spark algorithm for determining whether left and right are isomorphic
   */
  def isomorphism(left: Rdf#Graph, right: Rdf#Graph): Boolean = ???

  def graphSize(g: Rdf#Graph): Long = g.count()
}
