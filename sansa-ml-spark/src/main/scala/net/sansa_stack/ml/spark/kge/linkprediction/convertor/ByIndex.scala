package net.sansa_stack.ml.spark.kge.linkprediction.convertor

/**
 * ByIndex Class
 * -------------
 *
 * Created by lpfgarcia on 27/11/2017.
 */

import org.apache.spark.sql._

import net.sansa_stack.ml.spark.kge.linkprediction.triples.StringTriples
import net.sansa_stack.ml.spark.kge.linkprediction.triples.IntegerTriples

class ByIndex(data: Dataset[StringTriples], sk: SparkSession) extends Convertor(data: Dataset[StringTriples]) {

  val triples = numeric()

  import sk.implicits._

  def numeric() = {
    data.map { i =>
      IntegerTriples(e.indexOf(Row(i.Subject)) + 1, r.indexOf(Row(i.Predicate)) + 1,
        e.indexOf(Row(i.Object)) + 1)
    }
  }

}