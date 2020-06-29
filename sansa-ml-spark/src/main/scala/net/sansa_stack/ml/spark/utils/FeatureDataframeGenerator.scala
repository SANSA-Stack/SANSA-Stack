package net.sansa_stack.ml.spark.utils

import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

class FeatureDataframeGenerator {
  private val _available_modes = Array("an", "in", "on", "ar", "ir", "or", "at", "ir", "ot")
  private var _uri_to_features: Map[Node, Iterable[Seq[Node]]] = _
  private var _mode: String = _
  private var _all_uris: List[Node] = _
  private var _all_features: List[Seq[Node]] = _
  private var _number_uris: Int = _
  private var _number_features: Int = _
  private var _columns: List[String] = List("index", "feature")
  private var _node_to_index_map: Map[Node, Int] = _
  private var _index_to_node_map: Map[Int, Node] = _
  private var _nodeSeq_to_index_map: Map[Seq[Node], Int] = _
  private var _index_to_nodeSeq_map: Map[Int, Seq[Node]] = _
  private var _workaround_map: Map[Int, Iterable[Int]] = _

  def set_mode(mode: String): Unit = {
    if (_available_modes.contains(mode)) _mode = mode
    else println("The specified mode: " + mode + "is not supported. Currently available are: " + _available_modes)
  }
  def set_columns(cols: List[String]): Unit = _columns = cols
  def get_uris: List[Node] = _all_uris
  def get_features(node: Node): Iterable[Seq[Node]] = _uri_to_features(node)
  def workaround_get_fitted_map(): Map[Node, Iterable[Seq[Node]]] = _uri_to_features // better do this in transform, it is more concise then in perspective of pipelines
  def get_columns(): List[String] = _columns
  // def get_fitted_DF() =

  /* def transform(triples: RDD[Triple]): Map[Node, Iterable[Seq[Node]]] = {
    val generated_mapping: Map[Node, Iterable[Seq[Node]]] = triples
      .flatMap(triple => List(triple.getSubject, triple.getObject))
      .filter(_.isURI)
      .distinct()
      .filter(get_uris.contains(_)) // TODO drop this and change next line and it is working
      .map(n => (n, get_features(n))) // TODO .map(n => (n, List(Seq(n, n))))
      // https://stackoverflow.com/questions/22592811/task-not-serializable-java-io-notserializableexception-when-calling-function-ou
      .collect()
      .toMap
    val _not_covered_nodes = generated_mapping.keys.toSet.intersect(get_uris.toSet)
    if (_not_covered_nodes.size > 0) {
      println("transform will be performed but some nodes havn't been trained and so no fitting feature could be derived. missing uris are:")
      _not_covered_nodes.foreach(println(_))
    }
    generated_mapping
  } */

  def generate_uri_to_features(triples: RDD[Triple]): Map[Node, Iterable[Seq[Node]]] = {
    _mode match {
      case "at" => triples
        .flatMap(t => List((t.getSubject, Seq(t.getPredicate, t.getObject)), (t.getObject, Seq(t.getPredicate, t.getSubject))))
        .filter(_._1.isURI)
        .groupBy(_._1)
        .mapValues(_.map(_._2))
        .collect()
        .toMap
      case "it" => triples
        .flatMap(t => List((t.getObject, Seq(t.getPredicate, t.getSubject))))
        .filter(_._1.isURI)
        .groupBy(_._1)
        .mapValues(_.map(_._2))
        .collect()
        .toMap
      case "ot" => triples
        .flatMap(t => List((t.getSubject, Seq(t.getPredicate, t.getObject))))
        .filter(_._1.isURI)
        .groupBy(_._1)
        .mapValues(_.map(_._2))
        .collect()
        .toMap
      case "an" => triples
        .flatMap(t => List((t.getSubject, Seq(t.getObject)), (t.getObject, Seq(t.getSubject))))
        .filter(_._1.isURI)
        .groupBy(_._1)
        .mapValues(_.map(_._2))
        .collect()
        .toMap
      case "in" => triples
        .flatMap(t => List((t.getObject, Seq(t.getSubject))))
        .filter(_._1.isURI)
        .groupBy(_._1)
        .mapValues(_.map(_._2))
        .collect()
        .toMap
      case "on" => triples
        .flatMap(t => List((t.getSubject, Seq(t.getObject))))
        .filter(_._1.isURI)
        .groupBy(_._1)
        .mapValues(_.map(_._2))
        .collect()
        .toMap
      case "ar" => triples
        .flatMap(t => List((t.getSubject, Seq(t.getPredicate)), (t.getObject, Seq(t.getPredicate))))
        .filter(_._1.isURI)
        .groupBy(_._1)
        .mapValues(_.map(_._2))
        .collect()
        .toMap
      case "ir" => triples
        .flatMap(t => List((t.getObject, Seq(t.getPredicate))))
        .filter(_._1.isURI)
        .groupBy(_._1)
        .mapValues(_.map(_._2))
        .collect()
        .toMap
      case "or" => triples
        .flatMap(t => List((t.getSubject, Seq(t.getPredicate))))
        .filter(_._1.isURI)
        .groupBy(_._1)
        .mapValues(_.map(_._2))
        .collect()
        .toMap
      case _ => throw new Exception("This mode is currently not supported .\n You selected mode " + _mode + " .\n Currently available modes are: " + _available_modes)
    }
  }

  def fit(triples: RDD[Triple]): Unit = {
    _uri_to_features = generate_uri_to_features(triples)
    _all_uris = _uri_to_features.keys.toList
    _number_uris = _all_uris.size
    _node_to_index_map = (_all_uris zip (0 to _number_uris-1).toList).toMap
    _index_to_node_map = ((0 to _number_uris-1).toList zip _all_uris).toMap

    _all_features = _uri_to_features.map(_._2).flatten.toSet.toList
    _number_features = _all_features.size
    _nodeSeq_to_index_map = (_all_features zip (0 to _number_features-1).toList).toMap
    _index_to_nodeSeq_map = ((0 to _number_features-1).toList zip _all_features).toMap

    // this is a shortcut for transform TODO
    _workaround_map = _uri_to_features.
      map({case (node, iterable_node_seq) => (_node_to_index_map(node), iterable_node_seq.map(node_seq => _nodeSeq_to_index_map(node_seq)))})
      // .toSeq.
      // toDF(_columns)
  }

  def workaround_transform(): Map[Int, org.apache.spark.ml.linalg.Vector] = { // [Int, Seq[Tuple2[Int, Double]]]] = { // Map[Int, Array[Int]] = {
    // _workaround_map.mapValues(_.toArray)
    _workaround_map.mapValues(l => Vectors.sparse(_number_features, l.toSeq.map(v => (v, 1.0)))) // .mapValues(_.toSeq)
  }
} // TODO this should be DF in future and not Map
