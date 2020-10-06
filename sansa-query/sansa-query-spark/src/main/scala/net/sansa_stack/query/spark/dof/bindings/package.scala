package net.sansa_stack.query.spark.dof

import org.apache.jena.sparql.core.Var
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

package object bindings {
  type VarNodeMap[N] = Map[Var, N]
  type ResultRDD[N] = RDD[VarNodeMap[N]]
  type ResultDataset[N] = Dataset[VarNodeMap[N]]
}
