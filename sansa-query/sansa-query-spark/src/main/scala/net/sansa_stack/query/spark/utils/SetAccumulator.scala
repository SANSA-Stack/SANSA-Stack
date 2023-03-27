package net.sansa_stack.query.spark.utils

import org.apache.spark.util.AccumulatorV2

import java.util.Collections


/**
 * An [[AccumulatorV2 accumulator]] for collecting a set of elements.
 */
class SetAccumulator[T]
  extends AccumulatorV2[T, java.util.Set[T]] {

  private val _set: java.util.Set[T] = Collections.synchronizedSet(new java.util.HashSet[T]())

  /**
   * Returns false if this accumulator instance has any values in it.
   */
  override def isZero: Boolean = _set.isEmpty

  override def copyAndReset(): SetAccumulator[T] = new SetAccumulator

  override def copy(): SetAccumulator[T] = {
    val newAcc = new SetAccumulator[T]
    _set.synchronized {
      newAcc._set.addAll(_set)
    }
    newAcc
  }

  override def reset(): Unit = _set.clear()

  override def add(v: T): Unit = _set.add(v)

  override def merge(other: AccumulatorV2[T, java.util.Set[T]]): Unit = other match {
    case o: SetAccumulator[T] => _set.addAll(o.value)
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: java.util.Set[T] = _set.synchronized {
    java.util.Collections.unmodifiableSet(new java.util.HashSet[T](_set))
  }

  private[spark] def setValue(newValue: java.util.Set[T]): Unit = {
    _set.clear()
    _set.addAll(newValue)
  }
}