package net.sansa_stack.inference.flink.utils.key

/**
  * Base of a tuple-like generic key.
  *
  * @tparam T The type of the concrete key type.
  */
abstract class Key[T <: Key[T]] extends Comparable[T] {
  /**
    * Gets the i-th element of the tuple-like key.
    *
    * @param pos The position.
    * @return The element at that key position;
    */
  def get(pos: Int): Any
}


