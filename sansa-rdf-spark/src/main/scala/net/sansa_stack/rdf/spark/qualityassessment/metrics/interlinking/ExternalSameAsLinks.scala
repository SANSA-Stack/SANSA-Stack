package net.sansa_stack.rdf.spark.qualityassessment.metrics.interlinking

import net.sansa_stack.rdf.spark.qualityassessment.utils.NodeUtils._
import org.apache.jena.graph.{ Node, Triple }
import org.apache.jena.vocabulary.OWL
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author Gezim Sejdiu
 */
object ExternalSameAsLinks {

  @transient var spark: SparkSession = SparkSession.builder.getOrCreate()

  def assessExternalSameAsLinks(dataset: RDD[Triple]): Double = {

    val sameAsTriples = dataset.filter(f => checkLiteral(f.getPredicate).equals(OWL.sameAs))

    val triples = dataset.count().toDouble

    val sameAsCount = spark.sparkContext.longAccumulator("sameAsCount")

    sameAsTriples.filter(_.getObject.isURI()).foreach { f =>
      // if <local> owl:sameAs <external> or <external> owl:sameAs <local>
      val subjectIsLocal = isInternal(f.getSubject)
      val objectIsExternal = isExternal(f.getObject)

      if ((subjectIsLocal && objectIsExternal)
        || (!subjectIsLocal && !objectIsExternal)) {
        sameAsCount.add(1)
      }
    }

    val value = if (triples > 0.0) {
      sameAsCount.value / triples
    } else 0

    value
  }
}
