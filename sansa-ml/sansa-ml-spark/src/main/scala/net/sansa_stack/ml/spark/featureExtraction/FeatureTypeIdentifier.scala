package net.sansa_stack.ml.spark.featureExtraction

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Single
import net.sansa_stack.query.spark.SPARQLEngine
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.feature.{StopWordsRemover, StringIndexer, Tokenizer, VectorAssembler, Word2Vec, Word2VecModel}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, col, collect_list, collect_set, concat_ws, count, explode, explode_outer, max, mean, min, size, split, stddev}
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
    )
      .toDS()
      .persist()

    /*
    CREATE FEATURE EXTRACTING SPARQL
    from a knowledge graph we can either manually create a sparql query or
    we use the auto rdf2feature
     */

    println("\nCREATE FEATURE EXTRACTING SPARQL")
    val manualQueryString =
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
        |	# this fixes the entities, in this sample to be a movie
        | ?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> .
        |
        | # this is a optional block to gain only a smaller subset of entities, like Superhero-movies
        | # ?movie <http://data.linkedmdb.org/movie/genre> ?movie__down_genre .
        | # ?movie__down_genre <http://data.linkedmdb.org/movie/film_genre_name> "Superhero"
        |
        | # From here on we collect our Features
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

    val queryString = if (args(1) == "" ) manualQueryString else args(1)

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
      .persist()
    queryResultDf
      .show(false)
    /* val queryResultDfSize = queryResultDf.count()
    println(f"queryResultDf size ${}") */

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

    println("\nCOLLAPS COLUMNS & IDENTIFY FEATURE CHARACTERISTICS")
    // println(queryResultDf.schema)

    // specify column names
    val keyColumnNameString: String = "movie" // TODO setable
    val featureColumns: Seq[String] = List(queryResultDf.columns: _*).filter(!Set(keyColumnNameString).contains(_)).toSeq

    /**
     * This dataframe is a temporary df where we join to always a collapsed column for each feature
     */
    var collapsedDataframe: DataFrame = queryResultDf
      .select(keyColumnNameString)
      .dropDuplicates()
      .persist()

    queryResultDf.unpersist()

    val numberRows: Long = collapsedDataframe.count()
    println(s"Number distinct ids is: $numberRows")

    /**
     * In this Map we collect all gained inforamtion for every feature like type and so on
     */
    var featureDescriptions: mutable.Map[String, Map[String, Any]] = mutable.Map()

    featureColumns.foreach(
      currentFeatureColumnNameString => {
        println(currentFeatureColumnNameString)
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

        val joinableDf = {
          if (isListOfEntries) {
            collapsedTwoColumnDfwithSize
              .select(keyColumnNameString, currentFeatureColumnNameString)
          }
          else {
            twoColumnDf
              .select(keyColumnNameString, currentFeatureColumnNameString)
          }
        }
        collapsedDataframe = collapsedDataframe
          .join(joinableDf.withColumnRenamed(currentFeatureColumnNameString, f"${currentFeatureColumnNameString}(${featureType})"), keyColumnNameString)
      }
    )

    println("\nCOLLAPSED DATAFRAME")
    collapsedDataframe.show(false)

    println("\nFEATURE CHARACTERISTICS")
    featureDescriptions.foreach(println(_))

    /* val collapsedDfSize = collapsedDataframe.count()
    assert(collapsedDfSize == numberRows) TODO move to unit tests */

    println("\nDIGITIZE FEATURES")

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
      .persist()

    collapsedDataframe.unpersist()

    // val fullDigitizedDfSize = fullDigitizedDf.count()

    for (featureColumn <- collectedFeatureColumns) {

      // println(featureColumn)
      val featureType = featureColumn.split("\\(")(1).split("\\)")(0)
      val featureName = featureColumn.split("\\(")(0)
      println(featureName)
      println(featureType)

      val dfCollapsedTwoColumns = collapsedDataframe
        .select(keyColumnNameString, featureColumn)

      var digitizedDf: DataFrame = fullDigitizedDf
      var newFeatureColumnName: String = featureName

      if (featureType == "Single_NonCategorical_String") {

        val dfCollapsedTwoColumnsNullsReplaced = dfCollapsedTwoColumns
          .na.fill("")   // TODO NA FILL setable

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
          .setVectorSize(2)   // TODO Vector SIze setable
        val model = word2vec
          .fit(inputDf)
        digitizedDf = model
          .transform(inputDf)
          // .select(keyColumnNameString, "output")

        newFeatureColumnName += "(Word2Vec)"
      }
      else if (featureType == "ListOf_NonCategorical_String") {

        val dfCollapsedTwoColumnsNullsReplaced = dfCollapsedTwoColumns
          .withColumn("sentences", concat_ws(". ", col(featureColumn)))
          .na.fill("")   // TODO NA FILL setable

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
          .setVectorSize(2)  // TODO Vector SIze setable
        val model = word2vec
          .fit(inputDf)
        digitizedDf = model
          .transform(inputDf)
          // .select(keyColumnNameString, "output")

        newFeatureColumnName += "(Word2Vec)"
      }
      else if (featureType == "Single_Categorical_String") {

        val inputDf = dfCollapsedTwoColumns.na.fill("")  // TODO NA FILL setable

        val indexer = new StringIndexer()
          .setInputCol(featureColumn)
          .setOutputCol("output")

        digitizedDf = indexer.fit(inputDf).transform(inputDf)
        newFeatureColumnName += "(IndexedString)"
      }
      else if (featureType == "ListOf_Categorical_String") {

        val inputDf = dfCollapsedTwoColumns
          .select(col(keyColumnNameString), explode_outer(col(featureColumn)))
          .na.fill("")  // TODO NA FILL setable

        val indexer = new StringIndexer()
          .setInputCol("col")
          .setOutputCol("outputTmp")

        digitizedDf = indexer
          .fit(inputDf)
          .transform(inputDf)
          .groupBy(keyColumnNameString)
          .agg(collect_set("outputTmp") as "output")
          .select(keyColumnNameString, "output")
          // .join(dfCollapsedTwoColumns, keyColumnNameString) // TODO this is optional if we are interested in the pair of original and digitized feature representation
        newFeatureColumnName += "(ListOfIndexedString)"

      }
      else if (featureType.endsWith("Double")) {
        digitizedDf = dfCollapsedTwoColumns
          .withColumnRenamed(featureColumn, "output")
          .na.fill(-1.0) // TODO NA FILL setable
        newFeatureColumnName += s"(${featureType})"
      }
      else if (featureType.endsWith("Integer")) {
        digitizedDf = dfCollapsedTwoColumns
          .withColumn("output", col(featureColumn).cast(DoubleType))
          // .withColumnRenamed(featureColumn, "output")
          .na.fill(-1.0) // TODO NA FILL setable
        newFeatureColumnName += s"(${featureType})"
      }
      else if (featureType.endsWith("Boolean")) {
        digitizedDf = dfCollapsedTwoColumns
          .withColumn("output", col(featureColumn).cast(DoubleType))
          // .withColumnRenamed(featureColumn, "output")
          .na.fill(-1.0) // TODO NA FILL setable
        newFeatureColumnName += s"(${featureType})"
      }
      else {
        println("transformation not possible yet")
        digitizedDf = dfCollapsedTwoColumns
          .withColumnRenamed(featureColumn, "output")
        newFeatureColumnName += ("(notDigitizedYet)")
      }

      val joinableDf: DataFrame = digitizedDf
        .withColumnRenamed("output", newFeatureColumnName)
        .select(keyColumnNameString, newFeatureColumnName)

      fullDigitizedDf = fullDigitizedDf.join(
        joinableDf,
        keyColumnNameString
      )
    }

    val allColumns: Array[String] = fullDigitizedDf.columns
    val nonDigitizedCoulumns: Array[String] = allColumns.filter(_.contains("(notDigitizedYet)"))
    val digitzedColumns: Array[String] = allColumns diff nonDigitizedCoulumns

    if (nonDigitizedCoulumns.size > 0) println(s"we drop following non digitized columns:\n${nonDigitizedCoulumns.mkString("\n")}")
    // println("So simple digitized Dataframe looks like this:")
    val onlyDigitizedDf = fullDigitizedDf
      .select(digitzedColumns.map(col(_)): _*)

    fullDigitizedDf.unpersist()

    /* val reducedDfSize = onlyDigitizedDf.count()
    println(s"resulting dataframe has size ${reducedDfSize}")
    assert(reducedDfSize == fullDigitizedDfSize) TODO move to unit tests */
    onlyDigitizedDf.show()

    println("FIX FEATURE LENGTH")

    val columnsNameWithVariableFeatureColumnLength: Array[String] = onlyDigitizedDf.columns.filter(_.contains("ListOf"))

    var fixedLengthFeatureDf: DataFrame = onlyDigitizedDf
      .select((onlyDigitizedDf.columns diff columnsNameWithVariableFeatureColumnLength).map(col(_)): _*)
      .persist()

    // val fixedLengthFeatureDfSize = fixedLengthFeatureDf.count()

    for (columnName <- columnsNameWithVariableFeatureColumnLength) {
      println(s"Fix number of features in column: $columnName")

      val newColumnName: String = columnName.split("\\(")(0)

      val twoColumnDf = onlyDigitizedDf.select(keyColumnNameString, columnName)
      val fixedLengthDf = twoColumnDf
        .select(col(keyColumnNameString), explode_outer(col(columnName)))
        .groupBy(keyColumnNameString)
        .agg(
          mean("col").alias(s"${newColumnName}_mean"),
          min("col").alias(s"${newColumnName}_min"),
          max("col").alias(s"${newColumnName}_max"),
          stddev("col").alias(s"${newColumnName}_stddev"),
        )
        .na.fill(-1)

      fixedLengthFeatureDf = fixedLengthFeatureDf.join(fixedLengthDf, keyColumnNameString)
    }

    /* val sizeOffixedLengthFeatureDf = fixedLengthFeatureDf.count()
    assert(sizeOffixedLengthFeatureDf == fixedLengthFeatureDfSize) TODO Move to unit tests */
    fixedLengthFeatureDf.show(false)

    println("ASSEMBLE VECTOR")

    // TODO keep information about source for each vector entry s.t. it is explainable

    val columnsToAssemble: Array[String] = fixedLengthFeatureDf.columns.filterNot(_ == keyColumnNameString)
    println(s"columns to assemble:\n${columnsToAssemble.mkString(", ")}")

    val assembler = new VectorAssembler()
      .setInputCols(columnsToAssemble)
      .setOutputCol("features")
    val output = assembler
      .transform(fixedLengthFeatureDf)
      .persist()

    fixedLengthFeatureDf.unpersist()

    output.select(keyColumnNameString, "features").show(false)

    val outputSize = output.count()
    println(s"assembled vector has number of samples: $outputSize")

  }
}
