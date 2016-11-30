package net.sansa_stack.rdf.spark.io

import java.io.{ByteArrayInputStream, File}

import org.apache.jena.graph.Triple
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
  * An N-Triple reader. One triple per line is assumed.
  *
  * @author Lorenz Buehmann
  */
object NTripleReader {

  /**
    * Loads an N-Triple file into an RDD.
    *
    * @param session the Spark session
    * @param file    the path to the N-Triple file
    * @return the RDD of triples
    */
  def load(session: SparkSession, file: File): RDD[Triple] = {
    session.sparkContext.textFile(file.getAbsolutePath).map(line =>
      RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(line.getBytes), Lang.NTRIPLES, null).next())
  }

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.config("spark.kryo.registrationRequired", "true")
      .config("spark.eventLog.enabled", "true")
      .config("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator"
      ))
      .config("spark.default.parallelism", "4")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    val rdd = NTripleReader.load(sparkSession, new File(args(0)))

    println(rdd.take(10).mkString("\n"))
  }

}
