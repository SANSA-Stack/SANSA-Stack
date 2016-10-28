package net.sansa_stack.owl.spark

import org.apache.spark.sql.Dataset


trait ConvertibleToDataset[T] {
  def asDataset: Dataset[T]
}
