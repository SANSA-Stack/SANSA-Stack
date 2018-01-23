package net.sansa_stack.rdf.spark.qualityassessment.metrics.interlinking

import org.apache.spark.rdd.RDD
import org.apache.jena.graph.{ Triple, Node }
import net.sansa_stack.rdf.spark.qualityassessment.utils.NodeUtils._
import net.sansa_stack.rdf.spark.utils.StatsPrefixes._
import org.apache.spark.sql.SparkSession

object ExternalSameAsLinks {

  implicit class ExternalSameAsLinksFunctions(dataset: RDD[Triple]) extends Serializable { self =>
    // @transient var spark: SparkSession = _
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._
    /**
     */
    def assessExternalSameAsLinks() = {

      val sameAsTriples = dataset.filter(f => checkLiteral(f.getPredicate).equals(OWL_SAME_AS))

      val triples = dataset.count().toDouble
      println("triples:" + triples)
      val data = 1 to 10
      val dataRDD = self.spark.sparkContext.parallelize(data)
      dataRDD.foreach(println(_))
      val sameAsCount = self.spark.sparkContext.longAccumulator("My Accumulator")
      // val sameAsCount = spark.sparkContext.longAccumulator("sameAs")

      sameAsTriples.filter(_.getObject.isURI()).map { f =>
        // if <local> owl:sameAs <external> or <external> owl:sameAs <local>
        val subjectIsLocal = isInternal(f.getSubject)
        val objectIsExternal = isExternal(f.getObject)

        if ((subjectIsLocal && objectIsExternal)
          || (!subjectIsLocal && !objectIsExternal)) {
          sameAsCount.add(sameAsCount.value)
        }
      }
      val value = if (triples > 0.0)
        sameAsCount.value / triples;
      else 0

      value

    }
  }
}
