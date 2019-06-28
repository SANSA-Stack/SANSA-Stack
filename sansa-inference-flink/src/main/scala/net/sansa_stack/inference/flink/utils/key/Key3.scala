package net.sansa_stack.inference.flink.utils.key

/**
  * A key with two key fields.
  *
  * @tparam T1 The type of the first field.
  * @tparam T2 The type of the second field.
  * @tparam T3 The type of the third field.
  */
class Key3[T1 <: Comparable[T1], T2 <: Comparable[T2], T3 <: Comparable[T3]](val value1: T1, val value2: T2, val value3: T3)
  extends Key[Key3[T1, T2, T3]]
    with Equals {

  def get(pos: Int): Any = pos match {
    case 0 =>
      value1
    case 1 =>
      value2
    case 2 =>
      value3
    case _ =>
      throw new IndexOutOfBoundsException
  }

  override def hashCode: Int = {
    val c1: Int = if (value1 == null) 0 else value1.hashCode
    val c2: Int = if (value2 == null) 0 else value2.hashCode
    val c3: Int = if (value3 == null) 0 else value3.hashCode
    c1 * 17 + c2 * 31 + c3 * 47
  }

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Key3[T1, T2, T3]]

  override def equals(obj: Any): Boolean =
    obj match {
      case that: Key3[T1, T2, T3] =>
        (this eq that) || (this.canEqual(that) && (value1 == that.value1) && (value2 == that.value2) && (value3 == that.value3))
      case _ => false
    }

  override def toString: String = s"Key3 ($value1, $value2, $value3)"

  def compareTo(o: Key3[T1, T2, T3]): Int = {
    val other1 = o.value1
    val other2 = o.value2
    val other3 = o.value3

    val c1 = if (value1 == null)
      if (other1 == null) 0 else -1
    else
      if (other1 == null) 1 else value1.compareTo(other1)

    if(c1 != 0) c1 else {
      val c2 = if (value2 == null)
        if (other2 == null) 0 else -1
      else
        if (other2 == null) 1 else value2.compareTo(other2)

      if(c2 != 0) c2 else {
        if (value3 == null)
          if (other3 == null) 0 else -1
        else
          if (other3 == null) 1 else value3.compareTo(other3)
      }

    }

  }
}
