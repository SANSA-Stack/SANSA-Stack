package net.sansa_stack.examples.spark.ml

import net.sansa_stack.ml.spark.featureExtraction.{FeatureExtractingSparqlGenerator, SmartVectorAssembler, SparqlFrame}
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.{NTripleReader, RDFDataFrameReader, RDFReader}
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession}

object LMDB_Pipeline {
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
    val confHadoop = org.apache.hadoop.mapreduce.Job.getInstance().getConfiguration
    confHadoop.setBoolean("sansa.rdf.parser.skipinvalid", true)

    /*
    READ IN DATA
     */
    val inputFilePath = "/Users/carstendraschner/Datasets/linkedmdb-latest-dump.nt.txt"
    // val df: DataFrame = spark.read.rdf(Lang.NTRIPLES)(inputFilePath).cache()
    // val dataset = spark.rdf(Lang.NTRIPLES)(inputFilePath).toDS().cache()
    val dataset = NTripleReader.load(
      spark,
      inputFilePath,
      stopOnBadTerm = ErrorParseMode.SKIP,
      stopOnWarnings = WarningParseMode.SKIP
    ).toDS().cache()
    println(f"READ IN DATA:\ndata consists of ${dataset.count()} triples")
    dataset.take(n = 10).foreach(println(_))

    val df: DataFrame = NTripleReader.load(
      spark,
      inputFilePath,
      stopOnBadTerm = ErrorParseMode.SKIP,
      stopOnWarnings = WarningParseMode.SKIP
    ).toDF.toDF(Seq("s", "p", "o"): _*).cache() // .toDF(Seq("s", "p", "o"): _*)
    df.show(false)
    // val df = dataset.rdd.toDF()
    /*
    CREATE FEATURE EXTRACTING SPARQL
    from a knowledge graph we can either manually create a sparql query or
    we use the auto rdf2feature
     */
    // OPTION 1
    val manualSparqlString =
    """
      |SELECT
      |?movie ?title
      |
      |WHERE {
      |	?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> .
      |
      | OPTIONAL {
      |   ?movie <http://purl.org/dc/terms/title> ?title
      | }
      |}
      """.stripMargin
    // OPTION 2
    /* val (autoSparqlString: String, var_names: List[String]) = FeatureExtractingSparqlGenerator.createSparql(
      df,
      "?movie",
      "?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> .",
      0,
      3,
      10,
      featuresInOptionalBlocks = true,
    )
    print(autoSparqlString)

     */

    // select the query you want to use or adjust the automatic created one
    println("CREATE FEATURE EXTRACTING SPARQL")
    val queryString = manualSparqlString // autoSparqlString // manualSparqlString
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
      .setQueryExcecutionEngine("sparqlify")
    val res = sparqlFrame.transform(dataset)
    res.show(false)

    /*
    Create Numeric Feature Vectors
    */
    println("SMART VECTOR ASSEMBLER")
    val smartVectorAssembler = new SmartVectorAssembler()
      .setEntityColumn("movie")
      // .setLabelColumn("accidentId__down_hasEnvironmentDescription__down_lightingCondition")
    val assembledDf = smartVectorAssembler.transform(res).cache()
    assembledDf.show(false)
    println(f"assembled df has ${assembledDf.count()} rows")

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
