package net.sansa_stack.query.tests

import scala.util.control.NonFatal

/**
 * @author Lorenz Buehmann
 */
package object util {

  def withResources[T <: AutoCloseable, V](r: => T)(f: T => V): V = {
    val resource: T = r
    require(resource != null, "resource is null")
    var exception: Throwable = null
    try {
      f(resource)
    } catch {
      case e: Throwable =>
        exception = e
        throw e
    } finally {
      closeAndAddSuppressed(exception, resource)
    }
  }

  private def closeAndAddSuppressed(e: Throwable, resource: AutoCloseable): Unit = {
    if (e != null) {
      try {
        resource.close()
      } catch {
        case NonFatal(suppressed) =>
          e.addSuppressed(suppressed)
        case fatal: Throwable if NonFatal(e) =>
          fatal.addSuppressed(e)
          throw fatal
        case fatal: InterruptedException =>
          fatal.addSuppressed(e)
          throw fatal
        case fatal: Throwable =>
          e.addSuppressed(fatal)
      }
    } else {
      resource.close()
    }
  }

}
