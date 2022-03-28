package net.sansa_stack.ml.spark.similarity.MmDistSim

import java.util.Calendar

import net.sansa_stack.ml.spark.featureExtraction.{FeatureExtractingSparqlGenerator, SparqlFrame}
import net.sansa_stack.ml.spark.utils.SimilarityExperimentMetaGraphFactory
import net.sansa_stack.query.spark.SPARQLEngine
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.sys.JenaSystem
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, collect_set, desc, greatest, struct, sum, udf}
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object TryOutMmDistSim {
  def main(args: Array[String]): Unit = {

    val startTime: Long = System.nanoTime
    var currentTime: Long = System.nanoTime
    println("\nSETUP SPARK SESSION")
    // setup spark session
    // setup for IDE VM arguments:
    // -Dspark.master=local[*] -Dspark.driver.maxResultSize=4g -Dspark.executor.maxResultSize=4g -Dspark.driver.memory=4g -Dspark.executor.memory=4g -Dspark.memory.offHeap.size=4g -Dspark.memory.offHeap.enabled=true -Xms4g -Xmx4g
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
    println(f"\ntime needed: ${(System.nanoTime - currentTime) / 1e9d}")
    currentTime = System.nanoTime

    /*
    READ IN DATA
     */
    // spark.rdf(Lang.NTRIPLES)(args(0)).toDS().foreach(println(_))

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
    val numberTriples = dataset.count()
    println(f"\ndata consists of ${numberTriples} triples")
    dataset.take(n = 10).foreach(println(_))

    println(f"\ntime needed: ${(System.nanoTime - currentTime) / 1e9d}")
    currentTime = System.nanoTime

    /*
    CREATE FEATURE EXTRACTING SPARQL
    from a knowledge graph we can either manually create a sparql query or
    we use the auto rdf2feature
     */

    println("\nCREATE FEATURE EXTRACTING SPARQL")

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

    println(autoSparqlString)
    println(autoSparqlString.replace("\n", " "))


    // OPTION 2
    val minimalSparql = """
                          | SELECT
                          | ?movie
                          |
                          |WHERE {
                          |	?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> .
                          |}
    """.stripMargin

    val oneFeatureSparql = """
                             | SELECT
                             | ?movie ?movie__down_title
                             |
                             |WHERE {
                             |	?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> .
                             |
                             | OPTIONAL {
                             |		?movie <http://purl.org/dc/terms/title> ?movie__down_title .
                             |	}
                             |}
    """.stripMargin

    val manualSparqlString =
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

    // select the query you want to use or adjust the automatic created one

    val queryString = args(1) match {
      case "0" => minimalSparql
      case "1" => oneFeatureSparql
      case "2" => manualSparqlString
      case "3" => autoSparqlString
      case _ => args(1)
    } // autoSparqlString // manualSparqlString

    println()
    println(queryString)
    println(queryString.replace("\n", " "))

    println(f"\ntime needed: ${(System.nanoTime - currentTime) / 1e9d}")
    currentTime = System.nanoTime

    /*
    FEATURE EXTRACTION OVER SPARQL
    Gain Features from Query
    this creates a dataframe with columns corresponding to Sparql features
     */
    println("\nFEATURE EXTRACTION OVER SPARQL")
    val sparqlFrame = new SparqlFrame()
      .setSparqlQuery(queryString)
      .setQueryExcecutionEngine(SPARQLEngine.Sparqlify)
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

    // specify column names
    val keyColumnNameString: String = "movie"
    val featureColumns: Seq[String] = List(queryResultDf.columns: _*).filter(!Set(keyColumnNameString).contains(_)).toSeq
    println(s"feature columns $featureColumns")

    println(f"\ntime needed: ${(System.nanoTime - currentTime) / 1e9d}")
    currentTime = System.nanoTime


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

    println(s"In total we have we have: ${collectedDataFrame.count()} elements")
    println(s"Sample to see behavior on smaller data snippets")
    collectedDataFrame = collectedDataFrame
      // .sample(withReplacement = false, fraction = 0.1, seed = 504) // TODO remove in later version
    println(s"after sampling we have: ${collectedDataFrame.count()} elements")

    println(collectedDataFrame.schema)
    collectedDataFrame.show(false)

    println(f"\ntime needed: ${(System.nanoTime - currentTime) / 1e9d}")
    currentTime = System.nanoTime

    println(f"collectedDataFrame size ${collectedDataFrame.count()}")

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


    println("\nCREATE CROSS JOINED DF")
    /*
    cross join for all pair similairity
     */
    val tmpDf1 = collectedDataFrame.toDF(collectedDataFrame.columns.map(_ + "_1"): _*).cache()
    val tmpDf2 = collectedDataFrame.toDF(collectedDataFrame.columns.map(_ + "_2"): _*).cache()

    // create column name list to have them in paired order
    val crossJoinColumnNames: Seq[String] = tmpDf1.columns.zip(tmpDf2.columns).flatMap(t => Seq(t._1, t._2)).toSeq

    // drop diagonal entries
    // re order columns to have theim in pairs
    val crossJoinedDFs = tmpDf1.crossJoin(tmpDf2)
      // drop lines where same entities are present
      .filter(!(col(keyColumnNameString + "_1") === col(keyColumnNameString + "_2")))
      // reorder to have them in pairs
      .select(crossJoinColumnNames.map(col(_)): _*) // TODO remove in later version
      .cache()

    crossJoinedDFs.show(false) // TODO remove in later version
    // println(s"Full crossJoinedDFs size:  ${crossJoinedDFs.count()}") // TODO remove in later version

    // dataframe we operate on for similairty calculation
    var featureSimilarityScores: DataFrame = crossJoinedDFs.cache()

    // udf for naive Jaccard index as first placeholder
    val similarityEstimation = udf( (a: mutable.WrappedArray[Any], b: mutable.WrappedArray[Any]) => {
      val intersectionCard: Double = a.toSet.intersect(b.toSet).size.toDouble
      val unionCard: Double = a.toSet.union(b.toSet).size.toDouble
      val res = if (unionCard > 0) intersectionCard / unionCard else 0.0
      res
    })

    println(f"\ntime needed: ${(System.nanoTime - currentTime) / 1e9d}")
    currentTime = System.nanoTime

    println("\nWEIGHTS AND THRESHOLDS")
    // drop thresholds
    val minSimThresholds: mutable.Map[String, Double] = mutable.Map(featureColumns.map((cn: String) => Tuple2(cn, 0.0)).toMap.toSeq: _*)
    // TODO Temporal change
    minSimThresholds("movie__down_actor__down_actor_name") = 0.01 // TODO remove in later version

    // similairity column names
    val similairtyColumns = featureColumns.map(_ + "_Similarity")
    println(f"similairtyColumns:\n$similairtyColumns") // TODO remove in later version

    // these weights are user given or calculated
    val importance: mutable.Map[String, Double] = mutable.Map(featureColumns.map((cn: String) => Tuple2(cn, 1.0)).toMap.toSeq: _*)
    val reliability: mutable.Map[String, Double] = mutable.Map(featureColumns.map((cn: String) => Tuple2(cn, 1.0)).toMap.toSeq: _*)
    val availability: mutable.Map[String, Double] = mutable.Map(featureColumns.map((cn: String) => Tuple2(cn, 1.0)).toMap.toSeq: _*)

    // norm weights such that they sum up to one
    def normWeights(mapWeights: mutable.Map[String, Double]): Map[String, Double] = {
      val sum = mapWeights.map(_._2).reduce(_ + _)
      assert(sum > 0)
      mapWeights.map(kv => {(kv._1, kv._2/sum)}).toMap
    }

    // the normalized weight maps
    val importanceNormed: Map[String, Double] = normWeights(importance)
    val reliabilityNormed: Map[String, Double] = normWeights(reliability)
    val availabilityNormed: Map[String, Double] = normWeights(availability)
    println(f"\nIMPORTANTS WEIGHTS: importanceNormed:\n$importanceNormed")
    println(f"\nRELIABILITY WEIGHTS: reliabilityNormed:\n$reliabilityNormed")
    println(f"\nAVAILABILITY WEIGHTS: availabilityNormed:\n$availabilityNormed")
    println(f"\nMIN SIMILARITY THRESHOLDS:\n$minSimThresholds")



    val orderedFeatureColumnNamesByImportance: Seq[String] = ListMap(importanceNormed.toSeq.sortWith(_._2 > _._2): _*).map(_._1).toSeq
    println(f"orderedFeatureColumnNamesByImportance:\n$orderedFeatureColumnNamesByImportance \n")

    println(f"\ntime needed: ${(System.nanoTime - currentTime) / 1e9d}")
    currentTime = System.nanoTime

    /*
    Apply similairty score for pairs
     */
    println("\nCALCULATE COLUMN WISE SIMILARITY SCORES")
    orderedFeatureColumnNamesByImportance.foreach(
      featureName => {
        println(featureName) // TODO remove in later version
        // feature column name 1
        val fc1 = f"collect_set($featureName)_1"
        // feature column name 2
        val fc2 = f"collect_set($featureName)_2"
        // similarity column name
        val scn = f"${featureName}_Similarity"

        featureSimilarityScores = featureSimilarityScores
          .withColumn(
            scn,
            similarityEstimation(col(fc1), col(fc2))
          )
          // filter all rows where minimal similarity is not reached
          .filter(col(scn) >= minSimThresholds(featureName))
      }
    )

    println(f"\ntime needed: ${(System.nanoTime - currentTime) / 1e9d}")
    currentTime = System.nanoTime

    // drop columns where no similarity at all is given
    println("\nDROP ROWS WHERE NO SIMILARITY IS GIVEN")
    featureSimilarityScores = featureSimilarityScores.filter(greatest(similairtyColumns.map(col): _*) > 0)
    // println(f"number of pair with similairty above 0 and above thresholds: ${featureSimilarityScores.count()}")
    featureSimilarityScores.show(false) // TODO remove in later version

    println(f"\ntime needed: ${(System.nanoTime - currentTime) / 1e9d}")
    currentTime = System.nanoTime

    // calculate overall similarity

    // calculated weighted sum of all similairties
    println("\nCALCULATED WEIGHTED SIMILAIRTY SUM")
    println("weight similairties")
    featureColumns.foreach(
      featureColumn => {
        println(featureColumn)
        val similairtyColumn = featureColumn + "_Similarity"
        featureSimilarityScores = featureSimilarityScores
          .withColumn(
            f"${similairtyColumn}_weighted",
            col(similairtyColumn) * importanceNormed(featureColumn) * reliabilityNormed(featureColumn) * availabilityNormed(featureColumn)
          )
      }
    )

    println("calc weighted sum")
    featureSimilarityScores = featureSimilarityScores
      .withColumn("overallSimilarity", similairtyColumns.map(s => s + "_weighted")
        .map(col).reduce(_ + _))
    featureSimilarityScores.show(false) // TODO remove in later version

    // println(s"Fitting similarity pairs: ${featureSimilarityScores.count()}")

    println(f"featureSimilarityScores size ${featureSimilarityScores.count()}")

    println(f"\ntime needed: ${(System.nanoTime - currentTime) / 1e9d}")
    currentTime = System.nanoTime


    // order desc
    /* println("\nORDER SIMIALRITIES DESC")
    println("order desc")
    featureSimilarityScores = featureSimilarityScores.orderBy(desc(featureSimilarityScores.columns.last))
    featureSimilarityScores.show(false) // TODO remove in later version

    println(f"\ntime needed: ${(System.nanoTime - currentTime) / 1e9d}")
    currentTime = System.nanoTime

     */

    // make results rdf
    println("\nSELECT ONLY NEEDED COLUMN")
    featureSimilarityScores = featureSimilarityScores.select(featureSimilarityScores.columns(0), featureSimilarityScores.columns(1), featureSimilarityScores.columns.last)
    featureSimilarityScores.show(false)

    println(f"\ntime needed: ${(System.nanoTime - currentTime) / 1e9d}")
    currentTime = System.nanoTime

    println("\nCREATE METAGRAPH")
    val mgc = new SimilarityExperimentMetaGraphFactory
    val semanticResult: Dataset[org.apache.jena.graph.Triple] = mgc.createRdfOutput(
      outputDataset = featureSimilarityScores
    )(
      modelInformationEstimatorName = "DaDistSim",
      modelInformationEstimatorType = "Similarity",
      modelInformationMeasurementType = "Mix"
    )(inputDatasetNumbertOfTriples = numberTriples, dataSetInformationFilePath = inputFilePath
    ).toDS() // .cache()

    println("Metagraph looks like")
    semanticResult.take(20).foreach(println(_))
    println(f"Resulting Semantic Annotated Similarity consists of ${semanticResult.count()} triples")

    val outputFolderPath = args(2)

    if (outputFolderPath != "") {
      val outputFilePath = f"${outputFolderPath}DaDistSimResult${Calendar.getInstance().getTime().toString.replace(" ", "")}"

      println(f"\ntime needed: ${(System.nanoTime - currentTime) / 1e9d}")
      currentTime = System.nanoTime

      println(f"\nSTORE METAGRAPH\nwrite resulting MG to ${outputFilePath}")
      semanticResult.rdd.coalesce(1).saveAsNTriplesFile(outputFilePath)

      println(f"\ntime needed: ${(System.nanoTime - currentTime) / 1e9d}")
      currentTime = System.nanoTime

    }
    println(f"\nTotal experiment time  needed: ${(System.nanoTime - startTime) / 1e9d}")

  }
}


