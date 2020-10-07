package net.sansa_stack.ml.spark.clustering.datatypes

/**
  * @param poi1
  * @param poi2
  * @param distance distance between poi1 and poi2
  */
case class Distance(poi1: Long, poi2: Long, distance: Double)

