package org.dissect.rdf.spark.model

import org.apache.jena.graph.{Triple => JenaTriple, Node => JenaNode, Node_ANY, Node_ANY, Node_Literal, Node_Blank, Node_URI}

/**
 * @author Nilesh Chakraborty <nilesh@nileshc.com>
 */
trait JenaSpark extends Jena with SparkRDD

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
import org.apache.jena.graph.{Triple => JenaTriple, Node => JenaNode, Node_ANY, Node_URI, Node_Blank, Node_Literal}

import org.apache.spark.rdd.RDD

trait SparkRDD extends RDF {
  type Graph = RDD[Triple]
}
