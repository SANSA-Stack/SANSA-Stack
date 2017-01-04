package net.sansa_stack.inference.utils

/**
  * Some utility methods for Scala collections.
  *
  * @author Lorenz Buehmann
  */
object CollectionUtils {

  /**
    * Converts a list of tuples into a multimap having as key the first element of each tuple.
    *
    * @param tuples the tuples
    * @return the multimap
    */
  def toMultiMap(tuples: Iterable[(String, String)]): Map[String, Set[String]] = {
    tuples.groupBy(e => e._1).mapValues(e => e.map(x => x._2).toSet).map(identity)
  }

}
