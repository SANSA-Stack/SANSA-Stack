package org.sansa.inference.spark.utils;

/**
  * @author Lorenz Buehmann
  */
object EntailmentRegimes {

  sealed abstract class EntailmentRegime() {}

  case object RDFS extends EntailmentRegime()
  case object OWL extends EntailmentRegime()

}
