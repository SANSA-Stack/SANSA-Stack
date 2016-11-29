package net.sansa_stack.rdf.spark.io

import java.io.{ByteArrayInputStream, File}

import org.apache.jena.graph.Triple
import org.apache.jena.riot.{Lang, RDFDataMgr, RIOT}
import org.apache.jena.riot.lang.RiotParsers
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
  * @author Lorenz Buehmann
  */
object NTripleReader {

  def load(session: SparkSession, file: File) : RDD[Triple] = {
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
