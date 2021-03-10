package net.sansa_stack.rdf.spark.utils

import com.typesafe.scalalogging

import java.io.{Closeable, IOException}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.slf4j.Logger

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

  def time[R](block: => (String, R)): R = {
    val t0 = System.nanoTime()
    val result = block._2    // call-by-name
    val t1 = System.nanoTime()
    println(s"${block._1} - Elapsed time: " + (t1 - t0) / 10e6 + "ms")
    result
  }

  def time[R](message: String)(block: => R): R = {
    time((message, block))
  }

  def time[R](startedMessage: String, finishedMessage: String)(block: => R): R = {
    println(startedMessage)
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println(s"$finishedMessage\nElapsed time: " + (t1 - t0) / 10e6 + "ms")
    result
  }

  def time[R](startedMessage: String, finishedMessage: String, logger: Logger)(block: => R): R = {
    logger.debug(startedMessage)
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    logger.debug(s"$finishedMessage\nElapsed time: " + (t1 - t0) / 10e6 + "ms")
    result
  }

  def time[R](startedMessage: String, finishedMessage: String, logger: scalalogging.Logger)(block: => R): R = {
    logger.debug(startedMessage)
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    logger.debug(s"$finishedMessage\nElapsed time: " + (t1 - t0) / 10e6 + "ms")
    result
  }

  /**
   * @see org.apache.commons.lang3.StringUtils.unwrap
   * @param s
   * @param quoteChar
   * @return
   */
  def unQuote(s: String, quoteChar: Char = '"'): String = {
    org.apache.commons.lang3.StringUtils.unwrap(s, quoteChar)
  }
}
