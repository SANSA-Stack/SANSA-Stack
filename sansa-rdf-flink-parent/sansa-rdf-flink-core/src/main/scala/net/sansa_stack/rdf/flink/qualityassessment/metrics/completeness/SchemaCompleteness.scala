package net.sansa_stack.rdf.flink.qualityassessment.metrics.completeness

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._
import org.apache.jena.graph.{ Triple, Node }
import net.sansa_stack.rdf.flink.qualityassessment.dataset.DatasetUtils
import net.sansa_stack.rdf.flink.data.RDFGraph

/**
 * This metric measures the ratio of the number of classes and relations
 * of the gold standard existing in g, and the number of classes and
 * relations in the gold standard.
 */
object SchemaCompleteness {
  @transient var env: ExecutionEnvironment = _

  def apply(rdfgraph: RDFGraph) = {

    /*
     -->Rule->Filter-->
   		select (?p2, o) where ?s p=rdf:type isIRI(?o); ?p2 ?o2
			-->Action-->
			S+=?p2 && SC+=?o
			-->Post-processing-->
			|S| intersect |SC| / |SC|
   */
    val dataset = rdfgraph.triples
    
    val p2_o = dataset.filter(f =>
      f.getPredicate.getLocalName.equals("type")
        && f.getObject.isURI())

    val S = p2_o.map(_.getPredicate).distinct()
    val SC = dataset.map(_.getObject).distinct()

    val S_intersection_SC = S.collect().intersect(SC.collect()).distinct

    val SC_count = SC.count()
    val S_intersection_SC_count = S_intersection_SC.size

    if (SC_count > 0) S_intersection_SC_count.toDouble / SC_count.toDouble
    else 0.00
  }
  
}