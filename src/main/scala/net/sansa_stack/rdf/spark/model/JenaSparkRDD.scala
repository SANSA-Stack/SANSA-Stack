package net.sansa_stack.rdf.spark.model

import org.apache.jena.graph.{Node_ANY, Node_Blank, Node_Literal, Node_URI, Node => JenaNode, Triple => JenaTriple}
import org.apache.jena.vocabulary.RDF
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{Graph => SparkGraph}

import scala.reflect.ClassTag

/**
 * The JenaSpark model works with Jena and Spark RDDs.
 *
 * @author Nilesh Chakraborty <nilesh@nileshc.com>
 */
trait JenaSparkRDD extends Jena with SparkRDD { type Blah = JenaSparkRDD }

trait JenaSparkRDDOps extends SparkRDDGraphOps[JenaSparkRDD] with JenaNodeOps[JenaSparkRDD] {
  override implicit protected def nodeTag = reflect.ClassTag(classOf[JenaSparkRDD#Node])

  override implicit protected def tripleTag = reflect.ClassTag(classOf[JenaSparkRDD#Triple])

  override implicit protected def uriTag = reflect.ClassTag(classOf[JenaSparkRDD#URI])
}

object JenaSparkRDDOps {
  def apply(sparkContext: SparkContext) = new JenaSparkRDDOps {
    @transient override val sparkContext: SparkContext = sparkContext
  }
}

trait JenaSparkGraphX extends Jena with SparkGraphX { type Blah = JenaSparkGraphX }

trait JenaSparkGraphXOps extends GraphXGraphOps[JenaSparkGraphX] with JenaNodeOps[JenaSparkGraphX] {
    override implicit protected def nodeTag = reflect.ClassTag(classOf[JenaSparkGraphX#Node])

    override implicit protected def tripleTag = reflect.ClassTag(classOf[JenaSparkGraphX#Triple])

    override implicit protected def uriTag = reflect.ClassTag(classOf[JenaSparkGraphX#URI])
}

object JenaSparkGraphXOps {
  def apply(sparkContext: SparkContext) = new JenaSparkGraphXOps {
    @transient override val sparkContext: SparkContext = sparkContext
  }
}

trait Jena extends RDF {
  // types related to the RDF data model
  type Triple = JenaTriple
  type Node = JenaNode
  type URI = Node_URI
  type BNode = Node_Blank
  type Literal = Node_Literal
  type Lang = String

  // types for the graph traversal API
  type NodeMatch = JenaNode
  type NodeAny = Node_ANY
}

trait SparkRDD extends RDF {
  type Blah <: SparkRDD
  type Graph = RDD[Blah#Triple]
}

trait SparkGraphX extends RDF {
  type Blah <: SparkGraphX
  type Graph = SparkGraph[Blah#Node, Blah#URI]
}