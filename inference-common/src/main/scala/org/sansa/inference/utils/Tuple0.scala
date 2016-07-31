package org.sansa.inference.utils

/**
  * A 0-tuple.
  *
  * @author Lorenz Buehmann
  */
object Tuple0 extends AnyRef with Product with Serializable{
  def productArity = 0

  def productElement(n: Int) = throw new IllegalStateException("No element")

  def canEqual(that: Any) = false

  override def toString() = "(_)"
}
