package net.sansa_stack.query.spark.api.impl

import net.sansa_stack.query.spark.api.domain.{JavaResultSetSpark, ResultSetSpark}
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.rdd.RDD

import scala.jdk.CollectionConverters._

class ResultSetSparkJavaWrapper(javaResultSetSpark: JavaResultSetSpark)
  extends ResultSetSpark {
  override def getResultVars: Seq[Var] = javaResultSetSpark.getResultVars.asScala.toList

  override def getBindings: RDD[Binding] = javaResultSetSpark.getRdd.toJavaRDD()
}
