package net.sansa_stack.rdf.spark.qualityassessment.metrics.syntacticvalidity

import org.apache.spark.sql.SparkSession
import org.apache.jena.graph.{ Triple, Node }
import org.apache.spark.rdd.RDD
import net.sansa_stack.rdf.spark.qualityassessment.dataset.DatasetUtils._

/**
 * Check if the incorrect numeric range for the given predicate and given class of subjects.
 * A user should specify the RDF class, the RDF property for which he would like to verify
 * if the values are in the specified range determined by the user.
 * The range is specified by the user by indicating the lower and the upper bound of the value.
 */
object LiteralNumericRangeChecker {

  @transient var spark: SparkSession = _

  def apply(dataset: RDD[Triple]) = {

    /*
   		 -->Rule->Filter-->
   		"select COUNT(?s) where ?s rdf:type o=Type .
			select COUNT(?s2) where ?s2 p=rdf:type o=Type ?s <"+ property +"> ?o . FILTER (?o > "+ lowerBound +" && ?o < "+ upperBound +") ."
			-->Action-->
			S+=?s && S2+=?s2	
			-->Post-processing-->
			|S| / |S2|
   */

    val s2 = dataset.filter(f =>
      f.getPredicate.getLocalName.contains("type")
        && f.getSubject.getLiteralLexicalForm.contains(subject)).cache()

    val s = s2.filter(f => f.getPredicate.getLiteralLexicalForm.contains(property)
      && (f.getObject.getLiteralValue.toString().toDouble > lowerBound && f.getObject.getLiteralValue.toString().toDouble < upperBound))

    val S = s.distinct().count()
    val S2 = s2.distinct().count()
    
    println("subject: " + subject + ", property:" + property + ", upperBound: " + upperBound)

    val accuracy = if (S2 > 0) S / S2 else 0

    accuracy
  }
}