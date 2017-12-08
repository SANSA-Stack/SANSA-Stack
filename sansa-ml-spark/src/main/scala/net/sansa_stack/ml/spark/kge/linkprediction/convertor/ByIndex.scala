package net.sansa_stack.ml.spark.kge.linkprediction.convertor

/**
 * ByIndex Class
 * -------------
 *
 * Created by lpfgarcia on 27/11/2017.
 */

import org.apache.spark.sql._

import net.sansa_stack.ml.spark.kge.linkprediction.dataframe._

class ByIndex(data: Dataset[StringRecord], sk: SparkSession) extends Convertor(data: Dataset[StringRecord]) {

  val df = numeric()

  import sk.implicits._

  def numeric() = {
    data.map { i =>
      IntegerRecord(e.indexOf(Row(i.Subject)) + 1, r.indexOf(Row(i.Predicate)) + 1,
        e.indexOf(Row(i.Object)) + 1)
    }
  }

}