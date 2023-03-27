package net.sansa_stack.query.spark.api.impl

import net.sansa_stack.query.spark.api.domain.ResultSetSpark
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.rdd.RDD

/**
 * @author Lorenz Buehmann
 */
class ResultSetSparkImpl(resultVars: Seq[Var],
                         bindings: RDD[Binding])
  extends ResultSetSpark {
  override def getResultVars: Seq[Var] = resultVars

  override def getBindings: RDD[Binding] = bindings
}
