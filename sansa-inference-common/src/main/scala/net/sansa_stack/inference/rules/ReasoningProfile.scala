package net.sansa_stack.inference.rules

/**
  * An enumeration of reasoning profiles, which are usually different sets of rules with different complexity.
  *
  * @author Lorenz Buehmann
  */
object ReasoningProfile extends Enumeration {
  type ReasoningProfile = Value
  val RDFS, OWL_HORST, OWL_RL, OWL_EL = Value

  /**
    * Returns the reasoning profile for the given name.
    * Compared to `withName` this method allows for case-insensitive matching. In addition, hyphens `-` will be
    * replaced by underscore `_`, e.g. `owl-horst` -> `owl_horst`.
    * @param n the name of the reasoning profile
    * @return the reasoning profile (if exists)
    * @throws NoSuchElementException if the name doesn't match any reasoning profile
    */
  def forName(n: String): ReasoningProfile =
    (values find (_.toString.equalsIgnoreCase(n.replace("-", "_"))) getOrElse {
      throw new java.util.NoSuchElementException(n)
    })
}
