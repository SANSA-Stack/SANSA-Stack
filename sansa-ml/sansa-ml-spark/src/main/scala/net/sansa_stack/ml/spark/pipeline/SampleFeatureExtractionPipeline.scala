package net.sansa_stack.ml.spark.pipeline

import net.sansa_stack.ml.spark.featureExtraction.{SmartVectorAssembler, SparqlFrame}
import net.sansa_stack.ml.spark.utils.FeatureExtractingSparqlGenerator
import net.sansa_stack.rdf.spark.io.{RDFDataFrameReader, RDFReader}
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SampleFeatureExtractionPipeline {
  def main(args: Array[String]): Unit = {
    // setup spark session
    val spark = SparkSession.builder
      .appName(s"SampleFeatureExtractionPipeline")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // we need Kryo serialization enabled with some custom serializers
      .config("spark.kryo.registrator", String.join(
        ", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"))
      // .config("spark.sql.crossJoin.enabled", true) // needs to be enabled if your SPARQL query does make use of cartesian product Note: in Spark 3.x it's enabled by default
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    JenaSystem.init()

    /*
    READ IN DATA
     */
    val inputFilePath = "/Users/carstendraschner/GitHub/SANSA-Stack/sansa-ml/sansa-ml-spark/src/main/resources/test.ttl"
    val df: DataFrame = spark.read.rdf(Lang.TURTLE)(inputFilePath).cache()
    val dataset = spark.rdf(Lang.TURTLE)(inputFilePath).toDS().cache()
    /*
    CREATE FEATURE EXTRACTING SPARQL
    from a knowledge graph we can either manually create a sparql query or
    we use the auto rdf2feature
     */
    // OPTION 1
    val manualSparqlString =
    """
      |SELECT ?seed ?seed__down_age ?seed__down_name
      |
      |WHERE {
      |	?seed a <http://dig.isi.edu/Person> .
      |
      |	OPTIONAL {
      |		?seed <http://dig.isi.edu/age> ?seed__down_age .
      |	}
      |	OPTIONAL {
      |		?seed <http://dig.isi.edu/name> ?seed__down_name .
      |	}
      |}""".stripMargin
    // OPTION 2
    val (autoSparqlString: String, var_names: List[String]) = FeatureExtractingSparqlGenerator.createSparql(df, "?seed", "?seed a <http://dig.isi.edu/Person> .", 1, 2, 3, featuresInOptionalBlocks = true)

    // select the query you want to use or adjust the automatic created one
    println("CREATE FEATURE EXTRACTING SPARQL")
    val queryString = autoSparqlString
    println()
    println(queryString)

    /*
    FEATURE EXTRACTION OVER SPARQL
    Gain Features from Query
    this creates a dataframe with coulms corresponding to Sparql features
     */
    println("FEATURE EXTRACTION OVER SPARQL")
    val sparqlFrame = new SparqlFrame()
      .setSparqlQuery(queryString)
      .setQueryExcecutionEngine("ontop")
    val res = sparqlFrame.transform(dataset)
    res.show()

    /*
    Create Numeric Feature Vectors
    */
    println("SMART VECTOR ASSEMBLER")
    val smartVectorAssembler = new SmartVectorAssembler()
      .setEntityColumn("seed")
      .setLabelColumn("seed__down_age")
    val assembledDf = smartVectorAssembler.transform(res)
    assembledDf.show(false)

    /*
    APPLY Common SPARK MLlib Example Algorithm
     */
    println("APPLY Common SPARK MLlib Example Algorithm")
    // Trains a k-means model.
    val kmeans = new KMeans().setK(2) // .setSeed(1L)
    val model = kmeans.fit(assembledDf.distinct())

    // Make predictions
    val predictions = model.transform(assembledDf)
    predictions.show(false)

    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
  }
}
