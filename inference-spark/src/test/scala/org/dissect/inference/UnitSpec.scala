package org.dissect.inference

import org.scalatest._

/**
  * Base class for all test in the project.
  *
  * @author Lorenz Buehmann
  */
abstract class UnitSpec extends FlatSpec with Matchers with OptionValues with Inside with Inspectors {

}
