package net.sansa_stack.ml.spark.utils

import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.spark.rdd.RDD

class NodeIndexer {
  private var _node_to_index_map: Map[Node, Int] = _
  private var _index_to_node_map: Map[Int, Node] = _
  private var _vocab_size: Int = _

  def fit(triples: RDD[Triple]): Unit = {
    val set_of_nodes: Array[Node] = triples
      .flatMap(t => List(t.getSubject, t.getPredicate, t.getObject)) // unfold triple
      .filter(_.isURI)
      .collect()

    _vocab_size = set_of_nodes.size
    val enumeration: List[Int] = (1 to _vocab_size).toList
    _node_to_index_map = (set_of_nodes zip enumeration).toMap
    _index_to_node_map = (enumeration zip set_of_nodes).toMap
  }
  def get_index(node: Node): Int = _node_to_index_map(node): Int
  def get_node(index: Int): Node = _index_to_node_map(index): Node
  def get_vocab_size(): Int = _vocab_size: Int
}
