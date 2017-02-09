package net.sansa_stack.inference.spark.abstraction

/**
  * @author Lorenz Buehmann
  */
trait IndividualType {
  /**
    * @return a set of concepts occurring in the type. Note that changing in
    *         this set will reflect in changing in the type.
    */
  def getConcepts: Set[String]

  /**
    * @return a set of successor roles occurring in the type. Note that
    *         changing in this set will reflect in changing in the type.
    */
  def getSuccessorRoles: Set[String]

  /**
    * @return a set of predecessor roles occurring in the type.Note that
    *         changing in this set will reflect in changing in the type.
    */
  def getPredecessorRoles: Set[String]

}
