package net.sansa_stack.query.spark.gremlinator

import org.apache.spark.sql.SparkSession

object Main extends App {

  val spark = SparkSession.builder()
    .appName(s"SANSA-Gremlinator")
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  val computerResult = GremlinatorSpark("SELECT * WHERE { ?s ?p ?o .} LIMIT 10")
}
