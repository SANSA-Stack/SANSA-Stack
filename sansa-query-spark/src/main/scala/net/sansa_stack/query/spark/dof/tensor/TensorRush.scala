package net.sansa_stack.query.spark.dof.tensor

import net.sansa_stack.query.spark.dof.triple.Reader
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object TensorRush {
  def apply(spark: SparkSession, models: TensorModels):
    TensorRush = new TensorRush(spark, models)
}

class TensorRush(spark: SparkSession, models: TensorModels) {

  def getModels: TensorModels = this.models
  def getSparkContext: SparkContext = this.spark.sparkContext

  def addTensor(path: String, id: Int): TensorModels = {
    val reader = new Reader(this.spark, path)
    val model = RDDTensor(spark, reader)
    models += (id -> model);
  }
}
