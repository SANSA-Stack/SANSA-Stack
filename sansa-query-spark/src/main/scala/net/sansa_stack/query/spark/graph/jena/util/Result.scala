package net.sansa_stack.query.spark.graph.jena.util

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * A class represent one result for SPARQL query.
  *
  * @tparam VD Attribute of the key and value
  * @author Zhe Wang
  */
class Result[VD: ClassTag] extends Serializable {

  /**
    * Map from the variable to the value of result
    */
  private val solutionMapping = mutable.HashMap.empty[VD, VD]

  /**
    * All variable fields of a result
    */
  private val variableField = mutable.Set.empty[VD]

  /**
    * Add one mapping to the result line.
    */
  def addMapping(variable: VD, value: VD): Result[VD] = {
    if(variable.toString.startsWith("?")){
      if(variableField.contains(variable)) {
        // Variable is already in the field, add nothing.
      }
      else{
        variableField.add(variable)
        solutionMapping.put(variable, value)
      }
    }
    else {  // Not a variable, add nothing.
    }
    this
  }

  /**
    * Add several maps to the result.
    * @param map Map to add
    */
  def addAllMapping(map: Map[VD, VD]): Result[VD] = {
    map.foreach{ case(k, v) => addMapping(k, v) }
    this
  }

  override def equals(obj: scala.Any): Boolean = {
    if (!obj.isInstanceOf[Result[VD]]) {
      false
    } else{
      val other = obj.asInstanceOf[Result[VD]]
      if(this.solutionMapping.isEmpty) { false }
      else{
        if(this.solutionMapping.equals(other.solutionMapping)) { true }
        else { false }
      }
    }
  }

  def getMapping: Map[VD, VD] = {
    solutionMapping.toMap
  }

  def getField: Set[VD] = {
    variableField.toSet
  }

  def getValue(variable: VD): VD = {
    solutionMapping(variable)
  }

  override def hashCode(): Int = {
    solutionMapping.hashCode()
  }

  override def toString: String = {
    val line = new mutable.StringBuilder()
    solutionMapping.foreach{ case(k, v) =>
      if(line.isEmpty) {
        line.append(k.toString)
        line.append(": "+v.toString)
      } else {
        line.append("\t"+k.toString)
        line.append(": "+v.toString)
      }
    }
    line.toString()
  }

  /**
    * Select the required variable fields of the result, remove mapping which key is not
    * in the project field.
    */
  def project(projectField: Set[VD]): Result[VD] = {
    val removeField = variableField.diff(projectField)
    removeAllMapping(removeField.toSet)
    this
  }

  /**
    * Remove a set of variables from the result line.
    */
  def removeAllMapping(field: Set[VD]): Result[VD] = {
    field.foreach( v => removeMapping(v))
    this
  }

  /**
    * Remove one mapping from the result line.
    */
  def removeMapping(variable: VD): Result[VD] = {
    if(variableField.contains(variable)){
      variableField -= variable
      solutionMapping.remove(variable)
    }
    this
  }

  def returnLength(fields: Seq[VD]): Seq[Int] = {
    fields.map{ v =>
      val length = {
        if(variableField.contains(v)){
          solutionMapping(v).toString.length
        } else { 0 }
      }
      math.max(length, v.toString.length)
    }
  }
}
