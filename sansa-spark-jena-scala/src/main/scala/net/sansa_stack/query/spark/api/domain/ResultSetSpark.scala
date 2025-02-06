package net.sansa_stack.query.spark.api.domain

import org.apache.jena.sparql.algebra.{Table, TableFactory}
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.rdd.RDD

trait ResultSetSpark {
  def getResultVars: Seq[Var]
  def getBindings: RDD[Binding]

  /** Load the whole result set into a Jena table */
  def collectToTable(): Table = {
    import scala.jdk.CollectionConverters._
    val result = TableFactory.create(getResultVars.toList.asJava)
    getBindings.collect().foreach(b => result.addBinding(b))
    result
  }
}
