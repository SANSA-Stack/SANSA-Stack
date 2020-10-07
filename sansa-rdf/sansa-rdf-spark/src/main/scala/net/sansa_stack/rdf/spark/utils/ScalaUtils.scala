package net.sansa_stack.rdf.spark.utils

import java.io.{Closeable, IOException}

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

/**
  * @author Lorenz Buehmann
  */
object ScalaUtils extends Logging {
  /**
    * Execute a block of code that returns a value, re-throwing any non-fatal uncaught
    * exceptions as IOException. This is used when implementing Externalizable and Serializable's
    * read and write methods, since Java's serializer will not report non-IOExceptions properly;
    * see SPARK-4080 for more context.
    */
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        logError("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        logError("Exception encountered", e)
        throw new IOException(e)
    }
  }

  def tryWithResource[R <: Closeable, T](createResource: => R)(f: R => T): Try[T] = {
//    val resource = createResource
//    try f.apply(resource) finally if (resource != null) resource.close()
    cleanly(createResource)(_.close())(f)
  }

  def cleanly[A, B](resource: A)(cleanup: A => Unit)(doWork: A => B): Try[B] = {
    try {
      Success(doWork(resource))
    } catch {
      case e: Exception => Failure(e)
    }
    finally {
      try {
        if (resource != null) {
          cleanup(resource)
        }
      } catch {
        case e: Exception => println(e) // should be logged
      }
    }
  }
}
