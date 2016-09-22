package net.sansa.rdf.spark.utils
import org.slf4j.LoggerFactory

trait Logging {
  protected val logger = LoggerFactory.getLogger(getClass.getName)
}