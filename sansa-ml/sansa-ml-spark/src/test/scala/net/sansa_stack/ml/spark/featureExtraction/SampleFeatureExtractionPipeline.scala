package net.sansa_stack.ml.spark.featureExtraction

import net.sansa_stack.ml.spark.utils.FeatureExtractingSparqlGenerator
import org.apache.jena.riot.Lang
import org.apache.spark.sql.{DataFrame, SparkSession}
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model._
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}

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

    /*
    READ IN DATA
     */
    val inputFilePath = "/Users/carstendraschner/GitHub/SANSA-Stack/sansa-ml/sansa-ml-spark/src/main/resources/test.ttl"
    val df: DataFrame = spark.read.rdf(Lang.TURTLE)(inputFilePath).cache()
    val dataset = df.toDS()

    /*
    CREATE FEATURE EXTRACTING SPARQL
    from a knowledge graph we can either manually create a sparql query or
    we use the auto rdf2feature
     */
    // OPTION 1
    val manualSparqlString = """
                        |SELECT ?seed ?seed__down_age ?seed__down_name
                        |WHERE {
                        |?seed a <http://dig.isi.edu/Person> .
                        |OPTIONAL {
                        | ?seed <http://dig.isi.edu/age> ?seed__down_age .
                        |}
                        |OPTIONAL {
                        | ?seed <http://dig.isi.edu/name> ?seed__down_name .
                        |}}""".stripMargin
    // OPTION 2
    val (autoSparqlString: String, var_names: List[String]) = FeatureExtractingSparqlGenerator.autoPrepo(df, "?seed", "?seed a <http://dig.isi.edu/Person> .", 0, 2, 3)

    val queryString = manualSparqlString

    /*
    FEATURE EXTRACTION OVER SPARQL
    Gain Features from Query
    this creates a dataframe with coulms corresponding to Sparql features
     */
    val sparqlFrame = new SparqlFrame()
      .setSparqlQuery(queryString)
    val res = sparqlFrame.transform(dataset)
    res.show()

    /*
    COLLAPS MULTIPLE ROWS
    next step isnow to collaps rows with same entity identifier (first column)
    why this is needed: e.g. we might have feature age of parent.
    there we might get multiple rows for same element cause you might have two different parents with different ages
     */
    // TODO Strategy might differ relative to use case

    /*
    TRANSFORM TO NUMERIC VALUES
    we need to transform each column to numeric values for later feature vector
    strings can be hashed
     */
    val indexer = new StringIndexer()
      .setInputCol("seed__down_name")
      .setOutputCol("seed__down_name_Index")
    val indexed = indexer.fit(res).transform(res)
    indexed.show()

    /*
    ASSEMBLE VECTOR
    instead of having multiple column, we need in the end a single vector representing features
     */
    val assembler = new VectorAssembler()
      .setInputCols(Array("seed__down_age", "seed__down_name_Index"))
      .setOutputCol("features")
    val output = assembler.transform(indexed)
    output.select("seed", "features").show()
  }
}
