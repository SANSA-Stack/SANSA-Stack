package org.sansa.inference.flink.utils

import java.lang.Iterable

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

/**
  * @author Lorenz Buehmann
  */
object DataSetUtils {

  def subtract[T: ClassTag: TypeInformation](first: DataSet[T], second: DataSet[T]): DataSet[T] = {
    first.coGroup(second).where("*").equalTo("*")(new MinusCoGroupFunction[T](true)).name("subtract")
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
