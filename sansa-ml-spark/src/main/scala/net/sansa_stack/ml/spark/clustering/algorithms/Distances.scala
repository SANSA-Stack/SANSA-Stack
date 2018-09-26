package net.sansa_stack.ml.spark.clustering.algorithms

class Distances {

  /**
   * Jaccard Similarity Coefficient between two sets of categories corresponding to two pois
   *
   * @param x set of categories
   * @param y set of categories
   */
  def jaccardSimilarity(x: Set[String], y: Set[String]): Double = {
    val union_l = x.toList.length + y.toList.length
    val intersect_l = x.intersect(y).toList.length
    intersect_l / (union_l - intersect_l)
  }
}

