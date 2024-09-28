package net.sansa_stack.ml.spark.common

import org.apache.spark.sql.SparkSession

/** Class common to the ML test cases to set up kryo */
object CommonKryoSetup {
  val registrators: String = String.join(", ",
      "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
      "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator",
      "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify")

  def initKryoViaSystemProperties(): Unit = {
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.kryo.registrator", registrators)
  }

  def configureKryo(sc: SparkSession.Builder): SparkSession.Builder = {
    sc
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", registrators)
  }
}
