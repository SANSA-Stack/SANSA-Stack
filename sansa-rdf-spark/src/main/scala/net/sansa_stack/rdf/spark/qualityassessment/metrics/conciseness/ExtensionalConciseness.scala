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
        .map((_, 1L))
        .reduceByKey(_ + _)
        .map { case ((k, v), cnt) => (k, (v, cnt)) }
        .groupByKey()
        
  
      val mapSubjectsWithPredicatesValue = dataset.filter(triple => triple.getSubject.isURI() && triple.getPredicate.isURI())
      .map(f => (f.getSubject, f.getPredicate.getURI.toString() + " " + f.getObject.toString() + " "))
        .map((_, 1L))
        .reduceByKey(_ + _)  
        .map { case ((k, v), cnt) => (k, (v, cnt)) }
        .groupByKey()
        

      val test = mapSubjectsWithPredicates.map(v => (v, 1)).groupBy(_._1).mapValues(_.size)
      
      val ss = mapSubjectsWithPredicatesValue.map(x=>(x._1, x._2.groupBy(_._1).map(y=>(y._1, y._2.size))))

      /* val duplicates = mapSubjects.mapPartitions(f => {
      val triples = f
      })*/

      ss.collect().foreach(println(_))

      val s = mapSubjectsWithPredicates.mapPartitions(rows => {

        val row = rows.zipWithIndex
        row
      })

      println("ss : " + ss.count())
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