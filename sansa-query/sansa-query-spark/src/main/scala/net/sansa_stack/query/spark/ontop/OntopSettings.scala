package net.sansa_stack.query.spark.ontop

import java.util.Properties
import scala.util.{Failure, Success, Try}

case class OntopSettings(maxRowMappers: Int,
                         useLocalEvaluation: Boolean,
                         preInitializeWorkers: Boolean)

object OntopSettings {

  private val logger = com.typesafe.scalalogging.Logger[OntopSettings]

  val MAX_MAPPER_INSTANCE = "sansa.query.ontop.mapper.maxInstances"
  val EVALUATE_LOCAL = "sansa.query.ontop.evaluate.local"
  val PRE_INITIALIZE_WORKERS = "sansa.query.ontop.workers.preinitialize"

  def apply(properties: Properties): OntopSettings = {
    val maxRowMappers: Int = Try(Integer.parseInt(properties.getProperty(OntopSettings.MAX_MAPPER_INSTANCE))) match {
      case Success(value) => value
      case Failure(exception) =>
        logger.warn(s"Illegal value for property {$MAX_MAPPER_INSTANCE}, must be integer. Ignoring value.")
        -1
    }

    val useLocalEvaluation: Boolean = Try(java.lang.Boolean.parseBoolean(properties.getProperty(OntopSettings.EVALUATE_LOCAL))) match {
      case Success(value) => value
      case Failure(exception) =>
        logger.warn(s"Illegal value for property {$EVALUATE_LOCAL}, must be boolean. Ignoring value.")
        false
    }

    val preInitializeWorkers: Boolean = Try(java.lang.Boolean.parseBoolean(properties.getProperty(OntopSettings.PRE_INITIALIZE_WORKERS))) match {
      case Success(value) => value
      case Failure(exception) =>
        logger.warn(s"Illegal value for property {$PRE_INITIALIZE_WORKERS}, must be boolean. Ignoring value.")
        false
    }

    new OntopSettings(maxRowMappers, useLocalEvaluation, preInitializeWorkers)
  }


}
