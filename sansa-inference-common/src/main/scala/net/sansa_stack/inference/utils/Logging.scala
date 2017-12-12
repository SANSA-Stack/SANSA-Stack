package net.sansa_stack.inference.utils

import org.slf4j.{Logger, LoggerFactory}

import scala.language.implicitConversions

/**
  * Utility trait for classes that want to log data. Creates a SLF4J logger for the class and allows
  * logging messages at different levels using methods that only evaluate parameters lazily if the
  * log level is enabled.
  */
trait Logging {

  @transient private var log_ : Logger = null

  // Method to get or create the logger for this object
  protected def log: Logger = {
    if (log_ == null) {
      log_ = LoggerFactory.getLogger(logName)
    }
    log_
  }

  // Method to get the logger name for this object
  protected def logName = {
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")
  }


  def trace(msg: => String): Unit = { if (log.isTraceEnabled) log.trace(msg) }
  def trace(msg: => String, e: Throwable): Unit = { if (log.isTraceEnabled) log.trace(msg, e) }
  def trace(msg: => String, o: Any, os: Any*): Unit = { if (log.isTraceEnabled) log.trace(msg, o, os) }

  def debug(msg: => String): Unit = { if (log.isDebugEnabled) log.debug(msg) }
  def debug(msg: => String, e: Throwable): Unit = { if (log.isDebugEnabled) log.debug(msg, e) }
  def debug(msg: => String, o: Any, os: Any*): Unit = { if (log.isDebugEnabled) log.debug(msg, o, os) }

  def info(msg: => String): Unit = { if (log.isInfoEnabled)  log.info(msg) }
  def info(msg: => String, e: Throwable): Unit = { if (log.isInfoEnabled)  log.info(msg, e) }
  def info(msg: => String, o: Any, os: Any*): Unit = { if (log.isInfoEnabled)  log.info(msg, o, os) }

  def warn(msg: => String): Unit = { if (log.isWarnEnabled)  log.warn(msg) }
  def warn(msg: => String, e: Throwable): Unit = { if (log.isWarnEnabled)  log.warn(msg, e) }
  def warn(msg: => String, o: Any, os: Any*): Unit = { if (log.isWarnEnabled)  log.warn(msg, o, os) }

  def error(msg: => String): Unit = { if (log.isErrorEnabled) log.error(msg) }
  def error(msg: => String, e: Throwable): Unit = { if (log.isErrorEnabled) log.error(msg, e) }
  def error(msg: => String, o: Any, os: Any*): Unit = { if (log.isErrorEnabled) log.error(msg, o, os) }
}

private object Logging {
  implicit def logging2Logger(anything: Logging): Logger = anything.log
}
