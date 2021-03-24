package net.sansa_stack.ml.spark.featureExtraction

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Single
import net.sansa_stack.query.spark.SPARQLEngine
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.feature.{StopWordsRemover, StringIndexer, Tokenizer, Word2Vec, Word2VecModel}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, collect_set, concat_ws, count, explode, max, min, size, split}
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
        | ?movie__down_genre <http://data.linkedmdb.org/movie/film_genre_name> "Drama"
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
        /* val medianAlphaRatio: Double = datatype match {
          case StringType => twoColumnDf.
          case _ => 0
        }
        TODO nlp identification
         */

        val isCategorical: Boolean = if ((numberDistinctValues.toDouble / numberRows.toDouble) < 0.1) true else false

        var featureType: String = ""
        if (isListOfEntries) featureType += "ListOf_" else featureType += "Single_"
        if (isCategorical) featureType += "Categorical_" else featureType += "NonCategorical_"
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
        collapsedDataframe = collapsedDataframe.join(joinableDf.withColumnRenamed(currentFeatureColumnNameString, f"${currentFeatureColumnNameString}(${featureType})"), keyColumnNameString)
      }
    )
    collapsedDataframe.show(false)
    featureDescriptions.foreach(println(_))

    val collectedFeatureColumns: Seq[String] = List(collapsedDataframe.columns: _*).filter(!Set(keyColumnNameString).contains(_)).toSeq

    /*
    Strategies
    Boolean -> Double
    BooleanList -> DoubleList
    Double -> Double
    DoubleList -> DoubleList
    CategoricalString -> IndexedString
    CategoricalStringList -> IndexedStringList
    NlpString -> Word2VecDoubleVector
    NlpStringList -> Word2VecDoubleVectorList
     */
    var fullDigitizedDf: DataFrame = collapsedDataframe
      .select(keyColumnNameString)

    for (featureColumn <- collectedFeatureColumns) {

      println(featureColumn)
      val featureType = featureColumn.split("\\(")(1).split("\\)")(0)
      val featureName = featureColumn.split("\\(")(0)
      println(featureType)

      val dfCollapsedTwoColumns = collapsedDataframe
        .select(keyColumnNameString, featureColumn)

      var digitizedDf: DataFrame = fullDigitizedDf
      var newFeatureColumnName: String = featureName

      if (featureType == "Single_NonCategorical_String") {

        val dfCollapsedTwoColumnsNullsReplaced = dfCollapsedTwoColumns
          .na.fill("")

        val tokenizer = new Tokenizer()
          .setInputCol(featureColumn)
          .setOutputCol("words")

        val tokenizedDf = tokenizer
          .transform(dfCollapsedTwoColumnsNullsReplaced)

        val remover = new StopWordsRemover()
          .setInputCol("words")
          .setOutputCol("filtered")

        val inputDf = remover
          .transform(tokenizedDf)

        val word2vec = new Word2Vec()
          .setInputCol("filtered")
          .setOutputCol("output")
          .setVectorSize(10)
        val model = word2vec
          .fit(inputDf)
        digitizedDf = model
          .transform(inputDf)
          .select(keyColumnNameString, "output")

        newFeatureColumnName += "(Word2Vec)"
      }
      else if (featureType == "ListOf_NonCategorical_String") {

        val dfCollapsedTwoColumnsNullsReplaced = dfCollapsedTwoColumns
          .withColumn("sentences", concat_ws(". ", col(featureColumn)))
          .na.fill("")

        val tokenizer = new Tokenizer()
          .setInputCol("sentences")
          .setOutputCol("words")

        val tokenizedDf = tokenizer
          .transform(dfCollapsedTwoColumnsNullsReplaced)

        val remover = new StopWordsRemover()
          .setInputCol("words")
          .setOutputCol("filtered")

        val inputDf = remover
          .transform(tokenizedDf)

        val word2vec = new Word2Vec()
          .setInputCol("filtered")
          .setOutputCol("output")
          .setVectorSize(10)
        val model = word2vec
          .fit(inputDf)
        digitizedDf = model
          .transform(inputDf)
          .select(keyColumnNameString, "output")

        newFeatureColumnName += "(Word2Vec)"
      }
      else if (featureType == "Single_Categorical_String") {

        val inputDf = dfCollapsedTwoColumns

        val indexer = new StringIndexer()
          .setInputCol(featureColumn)
          .setOutputCol("output")

        digitizedDf = indexer.fit(inputDf).transform(inputDf)
        newFeatureColumnName += "(IndexedString)"
      }
      else if (featureType == "ListOf_Categorical_String") {

        val inputDf = dfCollapsedTwoColumns
          .select(col(keyColumnNameString), explode(col(featureColumn)))

        val indexer = new StringIndexer()
          .setInputCol(featureColumn)
          .setOutputCol("outputTmp")

        digitizedDf = indexer
          .fit(inputDf)
          .transform(inputDf).withColumn("output", collect_list(col("tmpOutput"))) //TODO better collect_list
        newFeatureColumnName += "(IndexedString)"
      }

      else {
        println("transformation not possible yet")
      }

      val joinableDf: DataFrame = digitizedDf
        .withColumnRenamed("output", newFeatureColumnName)

      joinableDf.show(false)

      fullDigitizedDf = fullDigitizedDf.join(
        joinableDf,
        keyColumnNameString
      )
    }
    fullDigitizedDf.show(false)
  }
}
