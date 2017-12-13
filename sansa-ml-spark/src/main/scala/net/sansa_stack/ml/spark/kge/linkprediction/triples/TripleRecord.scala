package net.sansa_stack.ml.spark.kge.linkprediction.triples

/**
 * Required triples case classes
 * -----------------------------
 *
 * Case classes for the Triples
 *
 * Created by Hamed Shariat Yazdi
 */

case class StringTriples(Subject: String, Predicate: String, Object: String)

case class IntegerTriples(Subject: Int, Predicate: Int, Object: Int)
