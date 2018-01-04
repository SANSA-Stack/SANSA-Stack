package net.sansa_stack.rdf.spark.qualityassessment.metrics.availability

import org.apache.spark.rdd.RDD
import org.apache.jena.graph.{ Triple, Node }
import net.sansa_stack.rdf.spark.qualityassessment.utils.NodeUtils._

/*
 * Dereferenceability of the URI.
 */
object DereferenceableUris {
  implicit class DereferenceableUrisFunctions(dataset: RDD[Triple]) extends Serializable {
    def assessDereferenceableUris() = {

      val totalURIs = dataset.filter(_.getSubject.isURI())
        .union(dataset.filter(_.getPredicate.isURI()))
        .union(dataset.filter(_.getObject.isURI()))
        .distinct().count().toDouble

      // check subject, if local and not a blank node
      val subjects = dataset.filter(f =>
        f.getSubject.isURI() && isInternal(f.getSubject) && !isBroken(f.getSubject)).count().toDouble

      // check predicate if local
      val predicates = dataset.filter(f =>
        f.getPredicate.isURI() && isInternal(f.getPredicate) && !isBroken(f.getPredicate)).count().toDouble

      // check object if URI and local
      val objects = dataset.filter(f =>
        f.getObject.isURI() && isInternal(f.getObject) && !isBroken(f.getObject)).count().toDouble

      val dereferencedURIs = subjects + predicates + objects

      val value = if (totalURIs > 0.0)
        dereferencedURIs / totalURIs;
      else 0

      value

    }
  }
}