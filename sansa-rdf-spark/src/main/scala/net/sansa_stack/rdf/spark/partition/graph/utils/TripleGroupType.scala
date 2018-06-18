package net.sansa_stack.rdf.spark.partition.graph.utils

object TripleGroupType extends Enumeration {
  type TripleGroupType = Value
  val s, o, so = Value
}
