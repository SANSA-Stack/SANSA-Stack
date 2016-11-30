package net.sansa_stack.inference.rules

/**
  * An enumeration of reasoning profiles, which are usually different sets of rules with different complexity.
  *
  * @author Lorenz Buehmann
  */
object ReasoningProfile extends Enumeration {
  type ReasoningProfile = Value
  val RDFS, OWL_HORST, OWL_RL, OWL_EL = Value

  def forName(n: String): ReasoningProfile =
    (values find (_.toString.equalsIgnoreCase(n.replace("-", "_"))) getOrElse {
      throw new java.util.NoSuchElementException(n)
    }).asInstanceOf[ReasoningProfile]
}
