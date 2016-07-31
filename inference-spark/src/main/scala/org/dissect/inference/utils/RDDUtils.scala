package org.dissect.inference.utils

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Utility class for RDD operations.
  *
  * @author Lorenz Buehmann
  */
object RDDUtils {


  implicit class RDDOps[T: ClassTag](rdd: RDD[T]) {

    def span(f: T => Boolean): (RDD[T], RDD[T]) = {
      val spaned = rdd.mapPartitions { iter =>
        val (left, right) = iter.span(f)
        val iterSeq = Seq(left, right)
        iterSeq.iterator
      }
      val left = spaned.mapPartitions { iter =>
        iter.next().toIterator
      }
      val right = spaned.mapPartitions { iter =>
        iter.next()
        iter.next().toIterator
      }
      (left, right)
    }

    def partitionBy(f: T => Boolean): (RDD[T], RDD[T]) = {
      val passes = rdd.filter(f)
      val fails = rdd.filter(e => !f(e)) // Spark doesn't have filterNot
      (passes, fails)
    }
  }

}
