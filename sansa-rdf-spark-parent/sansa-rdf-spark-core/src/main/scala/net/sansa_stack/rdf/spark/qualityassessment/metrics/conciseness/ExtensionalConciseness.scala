package net.sansa_stack.rdf.spark.qualityassessment.metrics.conciseness

import org.apache.spark.rdd.RDD
import org.apache.jena.graph.{ Triple, Node }
import net.sansa_stack.rdf.spark.qualityassessment.utils.NodeUtils._
import net.sansa_stack.rdf.spark.qualityassessment.vocabularies.DQV
import org.apache.spark.sql.Row

object ExtensionalConciseness {
  implicit class ExtensionalConcisenessFunctions(dataset: RDD[Triple]) extends Serializable {

    /**
     * The extensional conciseness
     * This metric metric checks for redundant resources in the assessed dataset,
     * and thus measures the number of unique instances found in the dataset.
     * @return  No. of unique subjects / Total No. of subjects
     */
    def assessExtensionalConciseness() = {

      val mapSubjects = dataset.map(_.getSubject).distinct()

      val mapSubjectsWithPredicates = dataset.filter(triple => triple.getSubject.isURI() && triple.getPredicate.isURI())
        .map(f => (f.getSubject, f.getPredicate))
        .groupByKey()

      val uniqueSubjects = 0
      //val uniqueSubjects = mapSubjects.mapPartitions(f, preservesPartitioning)

      /* .map { triple =>
          val subject = triple.getSubject
          val predicates = dataset.filter(_.getSubject.matches(triple.getSubject)).map(_.getPredicate)
          (subject, predicates)
        }*/
      val totalSubjects = mapSubjects.count().toDouble
      if (totalSubjects > 0) uniqueSubjects / totalSubjects else 0
    }
  }
}