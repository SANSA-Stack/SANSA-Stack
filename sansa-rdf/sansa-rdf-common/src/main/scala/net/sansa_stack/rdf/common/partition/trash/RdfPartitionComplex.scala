package net.sansa_stack.rdf.common.partition.core

import org.apache.jena.graph.Triple

import net.sansa_stack.rdf.common.partition.layout.TripleLayout

/**
 * FIXME Attributes have been merged with RdfPartitionDefault ; delete this file once system is sufficiently tested
 *
 * special datatypes: b for blank, u for uri, typed literal otherwise
 */
/*
case class RdfPartitionComplex(subjectType: Byte,
                               predicate: String,
                               objectType: Byte,
                               datatype: String,
                               langTagPresent: Boolean,
                               lang: Option[String],
                               partitioner: RdfPartitionerComplex)
  extends RdfPartition
    with Serializable {
  def layout: TripleLayout = partitioner.determineLayout(this)

  def matches(t: Triple): Boolean = {
    val p = partitioner.fromTriple(t)
    p == this
  }

  override def equals(o: Any): Boolean = o match {
    case RdfPartitionComplex(`subjectType`, `predicate`, `objectType`, `datatype`, `langTagPresent`, `lang`, _) => true
    case _ => false
  }
  override def hashCode: Int = (subjectType, predicate, objectType, datatype, langTagPresent, lang).##
}
*/
