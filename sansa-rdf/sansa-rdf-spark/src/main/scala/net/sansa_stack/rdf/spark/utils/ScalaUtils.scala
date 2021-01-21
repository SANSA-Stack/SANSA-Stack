package net.sansa_stack.rdf.spark.utils

import java.io.{Closeable, IOException}

import org.apache.spark.sql.catalyst.ScalaReflection

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{ClassSymbol, Type}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * @author Lorenz Buehmann
  * @author Claus Stadler
  */
object ScalaUtils extends Logging {

  /**
   * Return the scala type for a (java) class
   *
   * @param cls A (java) class
   * @return The scala Type instance
   */
  def getScalaType(cls: Class[_]): Type = {
    val classSymbol: ClassSymbol = universe.runtimeMirror(cls.getClassLoader).classSymbol(cls)
    val result: Type = classSymbol.toType

    result
  }

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
