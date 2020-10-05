package net.sansa_stack.query.spark.dof.tensor

import org.apache.spark.sql.SparkSession

object TensorStore {
  def instantiateUniqueStore(spark: SparkSession): TensorRush = {
    var models = TensorModels
    TensorRush(spark, models = models)
  }
}
