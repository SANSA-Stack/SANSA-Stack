package net.sansa_stack.ml.spark.similarity.MmDistSim

import net.sansa_stack.ml.spark.featureExtraction.{FeatureExtractingSparqlGenerator, SparqlFrame}
import net.sansa_stack.query.spark.SPARQLEngine
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, collect_set, greatest, struct, udf}
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object TryOutMmDistSim {
  def main(args: Array[String]): Unit = {
    // setup spark session
    val spark = SparkSession.builder
      .appName(s"SampleFeatureExtractionPipeline").master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // we need Kryo serialization enabled with some custom serializers
      .config("spark.kryo.registrator", String.join(
        ", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    JenaSystem.init()

    val inputFileString: String = args(0)
    println(f"Read data from: $inputFileString")

    val dataset = NTripleReader.load(
      spark,
      inputFileString,
      stopOnBadTerm = ErrorParseMode.SKIP,
      stopOnWarnings = WarningParseMode.IGNORE
    ).toDS().cache()
    println(f"READ IN DATA:\ndata consists of ${dataset.count()} triples")
    dataset.take(n = 10).foreach(println(_))

    /*
    CREATE FEATURE EXTRACTING SPARQL
    from a knowledge graph we can either manually create a sparql query or
    we use the auto rdf2feature
     */

    // OPTION 1
    val (autoSparqlString: String, var_names: List[String]) = FeatureExtractingSparqlGenerator.createSparql(
      dataset,
      "?movie",
      "?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> .",
      0,
      1,
      5,
      featuresInOptionalBlocks = true,
    )

    // OPTION 2
    val manualSparqlString =
      """
        | SELECT
        | ?movie
        | ?movie__down_date
        | ?movie__down_title
        | ?movie__down_runtime
        | ?movie__down_actor__down_actor_name
        | ?movie__down_genre__down_film_genre_name
        |
        |WHERE {
        |	?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> .
        |
        |	OPTIONAL {
        |		?movie <http://purl.org/dc/terms/date> ?movie__down_date .
        |	}
        |
        |	OPTIONAL {
        |		?movie <http://purl.org/dc/terms/title> ?movie__down_title .
        |	}
        |
        |	OPTIONAL {
        |		?movie <http://data.linkedmdb.org/movie/runtime> ?movie__down_runtime .
        |	}
        |
        | OPTIONAL {
        |		?movie <http://data.linkedmdb.org/movie/actor> ?movie__down_actor .
        |		?movie__down_actor <http://data.linkedmdb.org/movie/actor_name> ?movie__down_actor__down_actor_name .
        | }
        |
        | OPTIONAL {
        |		?movie <http://data.linkedmdb.org/movie/genre> ?movie__down_genre .
        |		?movie__down_genre <http://data.linkedmdb.org/movie/film_genre_name> ?movie__down_genre__down_film_genre_name .
        |	}
        |}
      """.stripMargin

    // select the query you want to use or adjust the automatic created one
    println("CREATE FEATURE EXTRACTING SPARQL")
    val queryString = manualSparqlString // autoSparqlString // manualSparqlString
    println()
    println(queryString)

    /*
    FEATURE EXTRACTION OVER SPARQL
    Gain Features from Query
    this creates a dataframe with columns corresponding to Sparql features
     */
    println("FEATURE EXTRACTION OVER SPARQL")
    val sparqlFrame = new SparqlFrame()
      .setSparqlQuery(queryString)
      .setQueryExcecutionEngine(SPARQLEngine.Sparqlify)
    val res = sparqlFrame.transform(dataset).filter(col("movie__down_genre__down_film_genre_name") === "Superhero").limit(10000).cache() // TODO remove limit
    res.show(false)

    /*
    Classify Column Type
    possible classes are:
    * boolean feature
    * numeric feature
    * datetime feature
    * categorical feature
    * string feature
    * boolean distribution
    * numeric distribution
    * datetime distribution
    * categorical feature list
    * string distribution
    * mixed distribution
     */
    /* major ideas:
    crete transformer which collects all data from dataframe
    and transformes those to their respective grouping type such that
    each entity has only one line
    also column names will encode feature type
     */
    val keyColumnNameString: String = "movie"
    val featureColumns: Seq[String] = List(res.columns: _*).filter(!Set(keyColumnNameString).contains(_)).toSeq
    println(s"feature columns $featureColumns")

    // val listOfCollectedDataframes = ListBuffer[DataFrame]()
    var collectedDataFrame = res.select(keyColumnNameString).dropDuplicates().cache()
    println("starting dataframe")

    for (currentFeatureColumnNameString <- featureColumns) {
      println(currentFeatureColumnNameString)
      val tmpDf = res.select(keyColumnNameString, currentFeatureColumnNameString)
      // tmpDf.show(false)
      val collectedTmpDf = tmpDf.groupBy(keyColumnNameString).agg(collect_set(currentFeatureColumnNameString)).as(currentFeatureColumnNameString)
      // collectedTmpDf.show(false)
      collectedDataFrame = collectedDataFrame.join(collectedTmpDf, keyColumnNameString)
    }

    println(collectedDataFrame.schema)



    val tmpDf1 = collectedDataFrame.toDF(collectedDataFrame.columns.map(_ + "_1"): _*)
    val tmpDf2 = collectedDataFrame.toDF(collectedDataFrame.columns.map(_ + "_2"): _*)

    val crossJoinColumnNames: Seq[String] = tmpDf1.columns.zip(tmpDf2.columns).flatMap(t => Seq(t._1, t._2)).toSeq

    val crossJoinedDFs = tmpDf1.crossJoin(tmpDf2)
      .filter(!(col(crossJoinColumnNames(0)) === col(crossJoinColumnNames(1))))
      .select(crossJoinColumnNames.map(col(_)): _*)
      .cache()
    crossJoinedDFs.show(false)

    var featureSimilarityScores: DataFrame = crossJoinedDFs.cache() // .select(crossJoinColumnNames(0), crossJoinColumnNames(1))

    val similarityEstimation = udf( (a: mutable.WrappedArray[Any], b: mutable.WrappedArray[Any]) => {

      val intersectionCard = a.toSet.intersect(b.toSet).size.toDouble
      val unionCard = a.toSet.union(b.toSet).size.toDouble
      val res = if (unionCard > 0) intersectionCard / unionCard else 0.0
      res
    })

    for (featureName <- featureColumns) {
      println(featureName)
      val fc1 = f"collect_set($featureName)_1"
      val fc2 = f"collect_set($featureName)_2"

      val scn = f"${featureName}_Similarity"

      featureSimilarityScores = featureSimilarityScores
        .withColumn(
          scn,
        similarityEstimation(col(fc1), col(fc2))
      )
        // .filter(col(scn) > 0)
    }

    val similairtyColumns = featureColumns.map(_ + "_Similarity")

    featureSimilarityScores.limit(20).show(false)

    featureSimilarityScores = featureSimilarityScores.filter(greatest(similairtyColumns.filter(_ != "movie__down_genre__down_film_genre_name_Similarity").map(col): _*) > 0)

    featureSimilarityScores.limit(20).show(false)
  }
}


