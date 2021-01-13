package net.sansa_stack.query.spark.api.domain

import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.rdd.RDD

trait ResultSetSpark {
  def getResultVars: Seq[Var]
  def getBindings: RDD[Binding]
}
