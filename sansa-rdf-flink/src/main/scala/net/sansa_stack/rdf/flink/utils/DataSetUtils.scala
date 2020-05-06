package net.sansa_stack.rdf.flink.utils

import java.lang.Iterable

import scala.reflect.ClassTag

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet
import org.apache.flink.table.runtime.IntersectCoGroupFunction
import org.apache.flink.util.Collector

/**
 * @author Lorenz Buehmann
 */
object DataSetUtils {

  implicit class DataSetOps[T: ClassTag: TypeInformation](dataset: DataSet[T]) {

    /**
     * Splits an RDD into two parts based on the given filter function. Note, that filtering is done twice on the same
     * data twice, thus, caching beforehand is recommended!
     *
     * @param f the boolean filter function
     * @return two Datasets
     */
    def partitionBy(f: T => Boolean): (DataSet[T], DataSet[T]) = {
      val passes = dataset.filter(f)
      val fails = dataset.filter((e: T) => !f(e)) // Flink doesn't have filterNot
      (passes, fails)
    }

    def subtract(other: DataSet[T]): DataSet[T] = {
      dataset.coGroup(other).where("*").equalTo("*")(new MinusCoGroupFunction[T](true)).name("subtract")
    }

    def intersect(other: DataSet[T]): DataSet[T] = {
      dataset.coGroup(other).where("*").equalTo("*")(new IntersectCoGroupFunction[T](true)).name("intersect")
    }
  }

}

class MinusCoGroupFunction[T: ClassTag: TypeInformation](all: Boolean) extends CoGroupFunction[T, T, T] {
  override def coGroup(first: Iterable[T], second: Iterable[T], out: Collector[T]): Unit = {
    if (first == null || second == null) return
    val leftIter = first.iterator
    val rightIter = second.iterator

    if (all) {
      while (rightIter.hasNext && leftIter.hasNext) {
        leftIter.next()
        rightIter.next()
      }

      while (leftIter.hasNext) {
        out.collect(leftIter.next())
      }
    } else {
      if (!rightIter.hasNext && leftIter.hasNext) {
        out.collect(leftIter.next())
      }
    }
  }
}
