package net.sansa_stack.inference.spark.data.loader.sql

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}

package object ntriples {
  /**
    * Adds a method, `ntriples`, to DataFrameWriter that allows you to write N-Triples files using
    * the `DataFrameWriter`
    */
  implicit class NTriplesDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def ntriples: String => Unit = writer.format("ntriples").save
  }

  /**
    * Adds a method, `ntriples`, to DataFrameReader that allows you to read n-Triples files using
    * the `DataFrameReader`
    */
  implicit class NTriplesDataFrameReader(reader: DataFrameReader) {
    def ntriples: String => DataFrame = reader.format("ntriples").load
  }
}