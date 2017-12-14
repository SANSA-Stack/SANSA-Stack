package net.sansa_stack.ml.spark.kge.linkprediction.prediction

/**
 * Predict Abstract Class
 * ----------------------
 *
 * Created by lpfgarcia on 14/11/2017.
 */

import org.apache.spark.sql._

import net.sansa_stack.rdf.spark.kge.triples.{StringTriples,IntegerTriples}

abstract class Evaluate(test: Dataset[IntegerTriples]) {

  def left(row: IntegerTriples, i: Int) = {
    IntegerTriples(i, row.Predicate, row.Object)
  }

  def right(row: IntegerTriples, i: Int) = {
    IntegerTriples(row.Subject, row.Predicate, i)
  }

  def rank(row: IntegerTriples, spo: String): Integer

  def ranking() = {

    var l, r = Seq[Integer]()

    test.collect().map { i =>
      l = rank(i, "l") +: l
      r = rank(i, "r") +: r
    }

    (l, r)
  }

  def rawHits10() = {

    var l, r = Seq[Boolean]()

    test.collect().map { row =>
      l = (rank(row, "l") <= 9) +: l
      r = (rank(row, "r") <= 9) +: r
    }

    (l, r)
  }

}