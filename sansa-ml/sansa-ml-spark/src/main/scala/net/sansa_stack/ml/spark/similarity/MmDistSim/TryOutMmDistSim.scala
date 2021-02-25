package net.sansa_stack.ml.spark.similarity.MmDistSim

import net.sansa_stack.ml.spark.featureExtraction.{FeatureExtractingSparqlGenerator, SparqlFrame}
import net.sansa_stack.query.spark.SPARQLEngine
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.sys.JenaSystem
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, collect_set, struct}
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}

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
        | ?movie__down_country__down_country_name
        | ?movie__down_country__down_country_languages
        | ?movie__down_country__down_country_areaInSqKm
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
    val res = sparqlFrame.transform(dataset).limit(20).cache() // TODO remove limit
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
    var collectedDataFrame = res.select(keyColumnNameString).dropDuplicates().limit(3).cache() // TODO remove limit if everthing is ready
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
      /* for (featureColumnName <- featureColumns) {
        tmpDf1 = tmpDf2.withColumnRenamed(featureColumnName, featureColumnName + "_1")
        tmpDf2 = tmpDf2.withColumnRenamed(featureColumnName, featureColumnName + "_2")
      }
       */

    val crossJoinColumnNames: Seq[String] = tmpDf1.columns.zip(tmpDf2.columns).flatMap(t => Seq(t._1, t._2)).toSeq

    val crossJoinedDFs = tmpDf1.crossJoin(tmpDf2)
      .filter(!(col(crossJoinColumnNames(0)) === col(crossJoinColumnNames(1))))
      .select(crossJoinColumnNames.map(col(_)): _*)
    crossJoinedDFs.show(false)

    /*
    Try out calculating single simialrity estimation of a pair of rows

    val row1: Row = collectedDataFrame.limit(2).first()
    val row2: Row = collectedDataFrame.limit(2).first()

    println(row1)
    println(row2)

    def prototypeSimilairty(r1: Row, r2: Row, featureColumns: Seq[String], oldSchema: StructType): Double = {

      val schemaR1: StructType = r1.schema
      val schemaR2: StructType = r2.schema

      println(schemaR1)
      println(schemaR2)

      for (currentFeatureColumnIndex <- 1 to featureColumns.size-1) {
        val currentFeatureColumnNameString = featureColumns(currentFeatureColumnIndex)
        println(currentFeatureColumnNameString)



        val sfR1: StructField = schemaR1(currentFeatureColumnIndex)
        val sfR2: StructField = schemaR2(currentFeatureColumnIndex)

        val typeR1: DataType = sfR1.dataType
        val typeR2: DataType = sfR2.dataType

        println(typeR1, typeR2)
        println(typeR1, typeR2)

        val innerTypeR1 = oldSchema(currentFeatureColumnIndex).dataType match {
          case StringType => String
          case IntegerType => Int
          case _ =>
        }
        println(f"inner data type $innerTypeR1")

        println(r1, r2, r1.schema, r2.schema)

        val fs1 = r1.getAs[collection.mutable.WrappedArray[innerTypeR1]](currentFeatureColumnIndex).toSeq
        val fs2 = r2.getAs[collection.mutable.WrappedArray[innerTypeR1]](currentFeatureColumnIndex).toSeq
        // println(currentFeatureColumnNameString)
        println(fs1, fs2, fs1.getClass, fs2.getClass)
        for (fe <- fs1) {
          println(fe, fe.getClass)
        }
        for (fe <- fs2) {
          println(fe, fe.getClass)
        }
      }
      0.0
    }

    prototypeSimilairty(row1, row2, featureColumns, res.schema) */
  }


}


