package net.sansa_stack.rdf.spark.utils

import org.slf4j.{Logger, LoggerFactory}

trait Logging {

    // Make the log field transient so that objects with Logging can
    // be serialized and used on another machine
    @transient private var log_ : Logger = null

    // Method to get the logger name for this object
    protected def logName = {
      // Ignore trailing $'s in the class names for Scala objects
      this.getClass.getName.stripSuffix("$")
    }

    // Method to get or create the logger for this object
    protected def log: Logger = {
      if (log_ == null) {
        log_ = LoggerFactory.getLogger(logName)
      }
      log_
    }

    // Log methods that take only a String
    protected def logInfo(msg: => String): Unit = {
      if (log.isInfoEnabled) log.info(msg)
    }

    protected def logDebug(msg: => String): Unit = {
      if (log.isDebugEnabled) log.debug(msg)
    }

    protected def logTrace(msg: => String): Unit = {
      if (log.isTraceEnabled) log.trace(msg)
    }

    protected def logWarning(msg: => String): Unit = {
      if (log.isWarnEnabled) log.warn(msg)
    }

    protected def logError(msg: => String): Unit = {
      if (log.isErrorEnabled) log.error(msg)
    }

    // Log methods that take Throwables (Exceptions/Errors) too
    protected def logInfo(msg: => String, throwable: Throwable): Unit = {
      if (log.isInfoEnabled) log.info(msg, throwable)
    }

    protected def logDebug(msg: => String, throwable: Throwable): Unit = {
      if (log.isDebugEnabled) log.debug(msg, throwable)
    }

    protected def logTrace(msg: => String, throwable: Throwable): Unit = {
      if (log.isTraceEnabled) log.trace(msg, throwable)
    }

    protected def logWarning(msg: => String, throwable: Throwable): Unit = {
      if (log.isWarnEnabled) log.warn(msg, throwable)
    }

    protected def logError(msg: => String, throwable: Throwable): Unit = {
      if (log.isErrorEnabled) log.error(msg, throwable)
    }

    protected def isTraceEnabled(): Boolean = {
      log.isTraceEnabled
    }
  }
