package net.sansa_stack.rdf.spark.qualityassessment.metrics.interlinking

import org.apache.spark.rdd.RDD
import org.apache.jena.graph.{ Triple, Node }
import net.sansa_stack.rdf.spark.qualityassessment.utils.NodeUtils._
import net.sansa_stack.rdf.spark.utils.StatsPrefixes._
import org.apache.spark.sql.SparkSession

/**
 * @author Gezim Sejdiu
 */
object ExternalSameAsLinks {

  implicit class ExternalSameAsLinksFunctions(dataset: RDD[Triple]) extends Serializable {
    // @transient var spark: SparkSession = _
    @transient var spark: SparkSession = SparkSession.builder.getOrCreate()
  
    def assessExternalSameAsLinks() = {

      val sameAsTriples = dataset.filter(f => checkLiteral(f.getPredicate).equals(OWL_SAME_AS))

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

      val value = if (triples > 0.0)
        sameAsCount.value / triples;
      else 0

      value
    }
  }
}
