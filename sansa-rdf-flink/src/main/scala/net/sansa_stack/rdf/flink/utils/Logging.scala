package net.sansa_stack.rdf.flink.utils

import org.slf4j.LoggerFactory

trait Logging {
  protected val logger = LoggerFactory.getLogger(getClass.getName)
}