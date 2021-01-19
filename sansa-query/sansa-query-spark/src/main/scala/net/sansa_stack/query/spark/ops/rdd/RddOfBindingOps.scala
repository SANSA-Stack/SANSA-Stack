package net.sansa_stack.query.spark.ops.rdd

import com.google.common.collect.Multiset
import org.aksw.jena_sparql_api.utils.BindingUtils
import org.apache.jena.datatypes.RDFDatatype
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.rdd.RDD

import scala.collection.{JavaConverters, mutable}

object RddOfBindingOps {

  /**
   * Return a new RDD[Binding] by projecting only the given variables
   *
   *
   * @param rddOfBindings The input RDD of bindings
   * @param projectVars The variables which to project
   * @return The RDD of bindings with the variables projected
   */
  def project(rddOfBindings: RDD[_ <: Binding], projectVars: Traversable[Var]): RDD[Binding] = {
    val varList = JavaConverters.seqAsJavaList(projectVars.toSeq)
    rddOfBindings.mapPartitions(_.map(BindingUtils.project(_, varList)))
  }

  /**
   * Collect all used datatype IRIs for each variable
   * mentioned in a RDD[Binding]
   *
   * IRI -> r2rml:IRI
   * BlankNode -> r2rml:BlankNode
   *
   * @return
   */
  def usedDatatypeIris(): mutable.Map[Var, Multiset[String]] = {
    null
  }

  /**
   *
   * @return
   */
  def usedIriPrefixes(rddOfBindings: RDD[_ <: Binding], bucketSize: Int = 1000): mutable.MultiMap[Var, RDFDatatype] = {
    null
  }


}
