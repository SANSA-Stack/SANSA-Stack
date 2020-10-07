package net.sansa_stack.inference.utils

/**
  * A 0-tuple.
  *
  * @author Lorenz Buehmann
  */
object Tuple0 extends AnyRef with Product with Serializable{
  def productArity: Int = 0

  def productElement(n: Int): Any = throw new IllegalStateException("No element")

  def canEqual(that: Any): Boolean = false

  override def toString: String = "(_)"
}
