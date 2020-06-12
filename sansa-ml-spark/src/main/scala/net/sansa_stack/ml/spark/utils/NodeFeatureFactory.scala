package net.sansa_stack.ml.spark.utils

import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.spark.rdd.RDD

class NodeFeatureFactory {
  private val _available_modes = Array("an", "in", "on", "ar", "ir", "or", "at", "ir", "ot")
  private var _uri_to_features: Map[Node, Iterable[Seq[Node]]] = _
  private var _mode: String = _
  private var  _all_uris: List[Node] = _

  def set_mode(mode: String): Unit = {
    if (_available_modes.contains(mode)) _mode = mode
    else println("The specified mode: " + mode + "is not supported. Currently available are: " + _available_modes)
  }
  def get_uris: List[Node] = _all_uris
  def get_features(node: Node): Iterable[Seq[Node]] = _uri_to_features(node)
  def get_fitted_map: Map[Node, Iterable[Seq[Node]]] = _uri_to_features // better do this in transform, it is more concise then in perspective of pipelines

  def transform(triples: RDD[Triple]): Map[Node, Iterable[Seq[Node]]] = {
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
  }

  def fit(triples: RDD[Triple]): Unit = {
    _mode match {
      case "at" => _uri_to_features = triples
        .flatMap(t => List((t.getSubject, Seq(t.getPredicate, t.getObject)), (t.getObject, Seq(t.getPredicate, t.getSubject))))
        .filter(_._1.isURI)
        .groupBy(_._1)
        .mapValues(_.map(_._2))
        .collect()
        .toMap
      case "it" => _uri_to_features = triples
        .flatMap(t => List((t.getObject, Seq(t.getPredicate, t.getSubject))))
        .filter(_._1.isURI)
        .groupBy(_._1)
        .mapValues(_.map(_._2))
        .collect()
        .toMap
      case "ot" => _uri_to_features = triples
        .flatMap(t => List((t.getSubject, Seq(t.getPredicate, t.getObject))))
        .filter(_._1.isURI)
        .groupBy(_._1)
        .mapValues(_.map(_._2))
        .collect()
        .toMap
      case "an" => _uri_to_features = triples
        .flatMap(t => List((t.getSubject, Seq(t.getObject)), (t.getObject, Seq(t.getSubject))))
        .filter(_._1.isURI)
        .groupBy(_._1)
        .mapValues(_.map(_._2))
        .collect()
        .toMap
      case "in" => _uri_to_features = triples
        .flatMap(t => List((t.getObject, Seq(t.getSubject))))
        .filter(_._1.isURI)
        .groupBy(_._1)
        .mapValues(_.map(_._2))
        .collect()
        .toMap
      case "on" => _uri_to_features = triples
        .flatMap(t => List((t.getSubject, Seq(t.getObject))))
        .filter(_._1.isURI)
        .groupBy(_._1)
        .mapValues(_.map(_._2))
        .collect()
        .toMap
      case "ar" => _uri_to_features = triples
        .flatMap(t => List((t.getSubject, Seq(t.getPredicate)), (t.getObject, Seq(t.getPredicate))))
        .filter(_._1.isURI)
        .groupBy(_._1)
        .mapValues(_.map(_._2))
        .collect()
        .toMap
      case "ir" => _uri_to_features = triples
        .flatMap(t => List((t.getObject, Seq(t.getPredicate))))
        .filter(_._1.isURI)
        .groupBy(_._1)
        .mapValues(_.map(_._2))
        .collect()
        .toMap
      case "or" => _uri_to_features = triples
        .flatMap(t => List((t.getSubject, Seq(t.getPredicate))))
        .filter(_._1.isURI)
        .groupBy(_._1)
        .mapValues(_.map(_._2))
        .collect()
        .toMap
      case _ => throw new Exception("This mode is currently not supported .\n You selected mode " + _mode + " .\n Currently available modes are: " + _available_modes)
      _all_uris = _uri_to_features.keys.toList
    }
  }
}