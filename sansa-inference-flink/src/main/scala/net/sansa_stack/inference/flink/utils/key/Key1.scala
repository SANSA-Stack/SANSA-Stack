package net.sansa_stack.inference.flink.utils.key

/**
  * A key with one key field.
  *
  * @tparam T1 The type of the field.
  */
class Key1[T1 <: Comparable[T1]](val value1: T1) extends Key[Key1[T1]] with Equals {

  def get(pos: Int): Any = pos match {
    case 0 =>
      value1
    case _ =>
      throw new IndexOutOfBoundsException
  }

  override def hashCode: Int = if (value1 == null) 0 else value1.hashCode

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Key1[T1]]

  override def equals(obj: Any): Boolean =
    obj match {
      case that: Key1[T1] => (this eq that) || (this.canEqual(that) && (value1 == that.value1))
      case _ => false
    }

  override def toString: String = s"Key1 ($value1)"

  def compareTo(o: Key1[T1]): Int = {
    val other = o.value1
    if (value1 == null)
      if (other == null) 0 else -1
    else
      if (other == null) 1 else value1.compareTo(other)
  }
}
