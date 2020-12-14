package net.sansa_stack.ml.spark.featureExtraction

import org.apache.jena.riot.Lang
import org.apache.spark.sql.{DataFrame, SparkSession}
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model._

object SampleFeatureExtractionPipeline {
  def main(args: Array[String]): Unit = {
    // setup spark session
    val spark = SparkSession.builder
      .appName(s"rdf2feature")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify",
        "net.sansa_stack.query.spark.ontop.KryoRegistratorOntop"))
      .config("spark.sql.crossJoin.enabled", true)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val inputFilePath = "/Users/carstendraschner/GitHub/SANSA-Stack/sansa-ml/sansa-ml-spark/src/main/resources/test.ttl"

    val queryString = """
                        |SELECT ?seed ?seed__down_age ?seed__down_name
                        |
                        |WHERE {
                        |?seed a <http://dig.isi.edu/Person>
                        |OPTIONAL {
                        | ?seed <http://dig.isi.edu/age> ?seed__down_age .
                        |}
                        |OPTIONAL {
                        | ?seed <http://dig.isi.edu/name> ?seed__down_name .
                        |}
                        |}""".stripMargin

    val sparqlFrame = new SparqlFrame()
      .setSparqlQuery(queryString = queryString)

    val df: DataFrame = spark.read.rdf(Lang.TURTLE)(inputFilePath).cache()
    val dataset = df.toDS()

    val res = sparqlFrame.transform(dataset)
    res.show()
  }
}
