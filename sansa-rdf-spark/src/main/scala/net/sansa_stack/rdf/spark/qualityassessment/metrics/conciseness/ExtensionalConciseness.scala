package net.sansa_stack.rdf.spark.qualityassessment.metrics.conciseness

import org.apache.spark.rdd.RDD
import org.apache.jena.graph.{ Triple, Node }
import net.sansa_stack.rdf.spark.qualityassessment.utils.NodeUtils._
import net.sansa_stack.rdf.spark.qualityassessment.vocabularies.DQV
import org.apache.spark.sql.Row

/**
 * @author Gezim Sejdiu
 */
object ExtensionalConciseness {
  implicit class ExtensionalConcisenessFunctions(dataset: RDD[Triple]) extends Serializable {

    /**
     * The extensional conciseness
     * This metric metric checks for redundant resources in the assessed dataset,
     * and thus measures the number of unique instances found in the dataset.
     * @return  No. of unique subjects / Total No. of subjects
     */
    def assessExtensionalConciseness() = {

      val mapSubjects = dataset.map(_.getSubject)

      val mapSubjectsWithPredicates = dataset.filter(triple => triple.getSubject.isURI() && triple.getPredicate.isURI())
        .map(f => (f.getSubject, f.getPredicate))
        .map((_, 1L))
        .reduceByKey(_ + _)
        .map { case ((k, v), cnt) => (k, (v, cnt)) }
        .groupByKey()

      val duplicateSubjects = dataset.filter(triple => triple.getSubject.isURI() && triple.getPredicate.isURI())
        .map(f => (f.getSubject, f.getPredicate.getURI.toString() + " " + f.getObject.toString() + " "))
        .map(f => (f._2, 1L))
        .reduceByKey(_ + _)
        .filter(_._2 > 1)
        .values.sum()

      // val duplicates = mapSubjectsWithPredicatesValue.map(x => (x._1, x._2.groupBy(_._1).map(y => (y._1, y._2.size))))

      val totalSubjects = mapSubjects.count().toDouble

      if (totalSubjects > 0) (totalSubjects - duplicateSubjects) / totalSubjects else 0
    }
  }
}