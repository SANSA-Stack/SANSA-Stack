package net.sansa_stack.ml.spark.clustering.datatypes

/**
 * Poi object representing a point of interest
 *
 * @param poi_id, id of poi
 * @param coordinate, coordinate of poi
 * @param categories, categories of poi
 */
case class Poi(poi_id: Long, coordinate: CoordinatePOI, categories: Categories, review: Double)

