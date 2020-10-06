package net.sansa_stack.inference.utils

/**
  * Some utils for logical combinations of boolean functions.
  */
object PredicateUtils {

  implicit class RichPredicate[A](f: A => Boolean) extends (A => Boolean) {
    def apply(v: A): Boolean = f(v)

    /**
      * Logical 'and'.
      *
      * @param g
      * @return
      */
    def &&(g: A => Boolean): A => Boolean = { x: A =>
      f(x) && g(x)
    }

    /**
      * Logical 'or'.
      *
      * @param g
      * @return
      */
    def ||(g: A => Boolean): A => Boolean = { x: A =>
      f(x) || g(x)
    }

    /**
      * Logical 'not'
      *
      * @return
      */
    def unary_! : A => Boolean = { x: A =>
      !f(x)
    }
  }
}
