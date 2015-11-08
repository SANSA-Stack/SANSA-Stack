package org.dissect.rdf.spark.model

import org.apache.spark.rdd.RDD

class Skos {
}

object Skos {
/*
   * printLabels - print the description of label for the specific keyword.
   * 
   * @param: literalPropsTriplesRDD- the literalspropRDD
   * @param: keyword- the search keyword containing on the label.
   * 
   */
  def printLabels(literalPropsTriplesRDD:RDD[(Long, Long, String)], keyword:String)
  {
     literalPropsTriplesRDD.filter(x => { x._3.contains(keyword) })
      .collect.foreach(println)
  }
  
}