package net.sansa_stack.inference.spark.utils

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

/**
  * Utility class for RDD operations.
  *
  * @author Lorenz Buehmann
  */
object RDDUtils {


  implicit class RDDOps[T: ClassTag](rdd: RDD[T]) {

    /**
      * Splits an RDD into two parts based on the given filter function
      *
      * @param f the boolean filter function
      * @return two RDDs
      */
    def span(f: T => Boolean): (RDD[T], RDD[T]) = {
      val spaned = rdd.mapPartitions { iter =>
        val (left, right) = iter.span(f)
        val iterSeq = Seq(left, right)
        iterSeq.iterator
      }
      val left = spaned.mapPartitions { iter =>
        iter.next()
      }
      val right = spaned.mapPartitions { iter =>
        iter.next()
        iter.next()
      }
      (left, right)
    }

    /**
      * Splits an RDD into two parts based on the given filter function. Note, that filtering is done twice on the same
      * data twice, thus, caching beforehand is recommended!
      *
      * @param f the boolean filter function
      * @return two RDDs
      */
    def partitionBy(f: T => Boolean): (RDD[T], RDD[T]) = {
      val passes = rdd.filter(f)
      val fails = rdd.filter(e => !f(e)) // Spark doesn't have filterNot
      (passes, fails)
    }
  }

}
