package net.sansa_stack.ml.spark.featureExtraction

import net.sansa_stack.query.spark.SPARQLEngine
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.sys.JenaSystem
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, collect_set, concat_ws, count, max, min, size}
import org.apache.spark.sql.types.{DataType, DoubleType, StringType}

import scala.collection.mutable

object FeatureTypeIdentifier {
  def main(args: Array[String]): Unit = {
    var currentTime: Long = System.nanoTime
    println("\nSETUP SPARK SESSION")
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

    println("\nREAD IN DATA")
    val inputFilePath = args(0)
    // val df: DataFrame = spark.read.rdf(Lang.NTRIPLES)(inputFilePath).cache()
    // val dataset = spark.rdf(Lang.NTRIPLES)(inputFilePath).toDS().cache()
    val dataset = NTripleReader.load(
      spark,
      inputFilePath,
      stopOnBadTerm = ErrorParseMode.SKIP,
      stopOnWarnings = WarningParseMode.IGNORE
    ).toDS().cache()

    /*
    CREATE FEATURE EXTRACTING SPARQL
    from a knowledge graph we can either manually create a sparql query or
    we use the auto rdf2feature
     */

    println("\nCREATE FEATURE EXTRACTING SPARQL")
    val queryString =
      """
        | SELECT
        | ?movie
        | ?movie__down_date
        | ?movie__down_title
        | ?movie__down_runtime
        | ?movie__down_actor__down_actor_name
        | ?movie__down_genre__down_film_genre_name
        | ?movie__down_country__down_country_name
        | ?movie__down_country__down_country_languages
        | ?movie__down_country__down_country_areaInSqKm
        |
        |WHERE {
        |	?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> .
        |
        | ?movie <http://data.linkedmdb.org/movie/genre> ?movie__down_genre .
        | ?movie__down_genre <http://data.linkedmdb.org/movie/film_genre_name> "Superhero"
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
        |
        | OPTIONAL {
        |		?movie <http://data.linkedmdb.org/movie/country> ?movie__down_country .
        |		?movie__down_country <http://data.linkedmdb.org/movie/country_name> ?movie__down_country__down_country_name .
        |	}
        |
        | OPTIONAL {
        |		?movie <http://data.linkedmdb.org/movie/country> ?movie__down_country .
        |		?movie__down_country <http://data.linkedmdb.org/movie/country_languages> ?movie__down_country__down_country_languages .
        |	}
        |
        | OPTIONAL {
        |		?movie <http://data.linkedmdb.org/movie/country> ?movie__down_country .
        |		?movie__down_country <http://data.linkedmdb.org/movie/country_areaInSqKm> ?movie__down_country__down_country_areaInSqKm .
        |	}
        |}
    """.stripMargin

    println(queryString)

    /*
    FEATURE EXTRACTION OVER SPARQL
    Gain Features from Query
    this creates a dataframe with columns corresponding to Sparql features
     */
    println("\nFEATURE EXTRACTION OVER SPARQL")
    val sparqlFrame = new SparqlFrame()
      .setSparqlQuery(queryString)
    val queryResultDf = sparqlFrame
      .transform(dataset)
      .cache()
    queryResultDf.show(false)
    println(f"queryResultDf size ${queryResultDf.count()}")

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

    println(queryResultDf.schema)

    // specify column names
    val keyColumnNameString: String = "movie"
    val featureColumns: Seq[String] = List(queryResultDf.columns: _*).filter(!Set(keyColumnNameString).contains(_)).toSeq

    var collapsedDataframe: DataFrame = queryResultDf
      .select(keyColumnNameString)
      .dropDuplicates()
      .cache()

    val numberRows: Long = collapsedDataframe.count()

    var featureDescriptions: mutable.Map[String, Map[String, Any]] = mutable.Map()

    featureColumns.foreach(
      currentFeatureColumnNameString => {
        val twoColumnDf = queryResultDf
          .select(keyColumnNameString, currentFeatureColumnNameString)
          .dropDuplicates()

        val groupedTwoColumnDf = twoColumnDf
          .groupBy(keyColumnNameString)

        val collapsedTwoColumnDfwithSize = groupedTwoColumnDf
          .agg(collect_list(currentFeatureColumnNameString) as currentFeatureColumnNameString)
          .withColumn("size", size(col(currentFeatureColumnNameString)))

        val minNumberOfElements = collapsedTwoColumnDfwithSize
          .select("size")
          .agg(min("size"))
          .head()
          .getInt(0)
        val maxNumberOfElements = collapsedTwoColumnDfwithSize
          .select("size")
          .agg(max("size"))
          .head()
          .getInt(0)

        val nullable: Boolean = if (minNumberOfElements == 0) true else false
        val datatype: DataType = twoColumnDf.select(currentFeatureColumnNameString).schema(0).dataType
        val numberDistinctValues: Int = twoColumnDf.select(currentFeatureColumnNameString).distinct.count.toInt
        val isListOfEntries: Boolean = if (maxNumberOfElements > 1) true else false
        val availability: Double = collapsedTwoColumnDfwithSize.select("size").filter(col("size") > 0).count().toDouble / numberRows.toDouble

        val isCategorical: Boolean = if (numberDistinctValues.toDouble / numberRows.toDouble < 0.001) true else false

        var featureType: String = ""
        if (isListOfEntries) featureType += "ListOf_" else featureType += "Single_"
        if (isCategorical) featureType += "categorical_" else featureType += "nonCategorical_"
        featureType += datatype.toString.split("Type")(0)

        val featureSummary: Map[String, Any] = Map(
          "featureType" -> featureType,
          "name" -> currentFeatureColumnNameString,
          "nullable" -> nullable,
          "datatype" -> datatype,
          "numberDistinctValues" -> numberDistinctValues,
          "isListOfEntries" -> isListOfEntries,
          "avalability" -> availability,
        )

        featureDescriptions(currentFeatureColumnNameString) = featureSummary

        // debug stuff
        // collapsedTwoColumnDfwithSize.filter(col("size") > 1).show()

        val joinableDf = {
          if (isListOfEntries) {
            collapsedTwoColumnDfwithSize
              .select(keyColumnNameString, currentFeatureColumnNameString)
          }
          else {
            queryResultDf
              .select(keyColumnNameString, currentFeatureColumnNameString)
          }
        }
        collapsedDataframe = collapsedDataframe.join(joinableDf, keyColumnNameString)
      }
    )
    collapsedDataframe.show(false)
    featureDescriptions.foreach(println(_))

    /*
    // collaps features into arrays instead of having multiple rows
    println("\nCOLLAPS FEATURES TO SETS")
    var collectedDataFrame = queryResultDf
      .select(keyColumnNameString)
      .dropDuplicates()
      .cache()

    featureColumns.foreach(
      currentFeatureColumnNameString =>
        collectedDataFrame = collectedDataFrame.join(
          queryResultDf
            .select(keyColumnNameString, currentFeatureColumnNameString)
            .groupBy(keyColumnNameString)
            .agg(collect_set(currentFeatureColumnNameString))
            .as(currentFeatureColumnNameString),
          keyColumnNameString)
    )

    println(collectedDataFrame.schema)
    collectedDataFrame.show(false)

    println("Feature Identification")
    /*
    In this step we expect a dataframe with column for features and labels and so on
    we want to resuta map where for each column it is defined, what kind of feature it is
    possible features
    - single element
      - numeric
      - boolean
      - categorical
      - nlp
    - multiple elemnt
      - categorical feature set
      - nlps
      - numeric distribution
      - numbers as category ids
     */
    // for this purpose we iterate over the columns
    for (collapsedColumn <- collectedDataFrame.columns) {
      println(collapsedColumn)
      /*
      needed steps
      - gain datatype of elements: eg: boolean, double, string, ...
      - collect number distribution
      - collect null value exists
       */

      // count values in collapsed lists for min and max to evaluate if it is feature set or something else
      collectedDataFrame
        .select(collapsedColumn)
        .rdd
        .map((row: Row) => row(1).asInstanceOf[Array[Any]])
        .map(_.size)
        .foreach(println(_))
    }

     */
  }
}
