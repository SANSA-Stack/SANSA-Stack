package net.sansa_stack.rdf.common.io.riot.error

import org.apache.jena.riot.SysRIOT
import org.apache.jena.riot.SysRIOT.fmtMessage
import org.apache.jena.riot.system.ErrorHandler
import org.slf4j.Logger


/**
 * A custom error handler that doesn't throw an exception on fatal parse errors. This allows for simply skipping those
 * triples instead of aborting the whole parse process.
 *
 * @param log an optional logger
 */
class CustomErrorHandler(val log: Logger = SysRIOT.getLogger)
  extends ErrorHandler {

  /** report a warning */
  def logWarning(message: String, line: Long, col: Long): Unit = {
    if (log != null) log.warn(fmtMessage(message, line, col))
  }

  /** report an error */
  def logError(message: String, line: Long, col: Long): Unit = {
    if (log != null) log.error(fmtMessage(message, line, col))
  }

  /** report a catastrophic error */
  def logFatal(message: String, line: Long, col: Long): Unit = {
    if (log != null) logError(message, line, col)
  }

  override def warning(message: String, line: Long, col: Long): Unit = logWarning(message, line, col)

  override def error(message: String, line: Long, col: Long): Unit = logError(message, line, col)

  override def fatal(message: String, line: Long, col: Long): Unit = logFatal(message, line, col)
}