package net.sansa_stack.examples.spark.ml.Similarity

import net.sansa_stack.ml.spark.featureExtraction.{FeatureExtractingSparqlGenerator, SmartVectorAssembler, SparqlFrame}
import net.sansa_stack.ml.spark.featureExtraction._
import net.sansa_stack.ml.spark.similarity.similarityEstimationModels.{JaccardModel, MinHashModel}
import net.sansa_stack.ml.spark.utils.{FeatureExtractorModel, ML2Graph}
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.{NTripleReader, RDFReader}
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.graph
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, MinMaxScaler, StandardScaler}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}

object DaSim {
  def main(args: Array[String]): Unit = {

    // readIn
    val input: String = args(0) // http://www.cs.toronto.edu/~oktie/linkedmdb/linkedmdb-18-05-2009-dump.nt

    println("\nSETUP SPARK SESSION")
    val spark = {
      SparkSession.builder
      .appName(s"SampleFeatureExtractionPipeline")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", String.join(", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"))
      .getOrCreate()
    }
    spark.sparkContext.setLogLevel("ERROR")
    JenaSystem.init()

    println("\nREAD IN DATA")
    /**
     * Read in dataset of Jena Triple representing the Knowledge Graph
     */
    var originalDataRDD: RDD[graph.Triple] = null
    if (input.endsWith("nt")) {
      originalDataRDD = NTripleReader
        .load(
          spark,
          input,
          stopOnBadTerm = ErrorParseMode.SKIP,
          stopOnWarnings = WarningParseMode.IGNORE
        )
    } else {
      val lang = Lang.TURTLE
      originalDataRDD = spark.rdf(lang)(input).persist()
    }
    val dataset: Dataset[graph.Triple] = originalDataRDD
      .toDS()
      .cache()

    println(f"\ndata consists of ${dataset.count()} triples")
    dataset
      .take(n = 10).foreach(println(_))

    /* println("FETCH SEEDS by SPARQL")

    val p_seed_fetching_sparql =
      """
        |SELECT ?seed
        |WHERE {
        |?seed <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> .
        |# ?seed <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/Person> .
        |}
        |""".stripMargin

    val sf = new SparqlFrame()
      .setSparqlQuery(p_seed_fetching_sparql)

    val seeds = sf
      .transform(dataset)
      .limit(100) // TODO this is temporary

    println("seeds by sparql")
    seeds.show(false)
    println(seeds.count())
     */

    println("FETCH SEEDS by filter")

    val seeds: DataFrame = dataset
      // .filter(t => ((t.getObject.toString().equals("http://data.linkedmdb.org/movie/film")) & (t.getPredicate.toString().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))))
      // .filter(t => ((t.getObject.toString().equals("http://dbpedia.org/ontology/Person"))))
      .filter(t => ((t.getObject.toString().equals("http://data.linkedmdb.org/movie/film"))))
      .limit(10)
      .rdd
      .toDF()
      .select("s")

    seeds
      .show(false)

    println("GATHER CANDIDATE PAIRS")

    implicit val rdfTripleEncoder: Encoder[Triple] = org.apache.spark.sql.Encoders.kryo[Triple]

    import spark.implicits._

    /* val seedsList = seeds
      .select("s")
      .map(r => r.toString())
      .collect()

    val filtered2 = dataset
      .filter(t => seedsList.contains(t.getSubject))
    filtered2
      .take(100)
      .foreach(println(_))
     */

    val filtered: Dataset[Triple] = seeds
      .rdd
      .map(r => Tuple2(r(0).toString, r(0)))
      .join(dataset.rdd.map(t => Tuple2(t.getSubject.toString(), t)))
      .map(_._2._2)
      .toDS()
      .as[Triple]

    println("the filtered kg has #triples:" + filtered.count())

    val triplesDf = filtered
      .rdd
      .toDF()

    // println("filtered KG")
    // triplesDf.show(false)

    val featureExtractorModel = new FeatureExtractorModel()
      .setMode("os")
    val extractedFeaturesDataFrame = featureExtractorModel
      .transform(triplesDf)

    extractedFeaturesDataFrame
      .show(false)

    // count Vectorization
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("extractedFeatures")
      .setOutputCol("vectorizedFeatures")
      .fit(extractedFeaturesDataFrame)
    val tmpCvDf: DataFrame = cvModel
      .transform(extractedFeaturesDataFrame)
    // val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 }, DataTypes.BooleanType)
    // val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 })
    val countVectorizedFeaturesDataFrame: DataFrame = tmpCvDf
      // .filter(isNoneZeroVector(col("vectorizedFeatures")))
      .select("uri", "vectorizedFeatures").cache()
    countVectorizedFeaturesDataFrame
      .show(false)

    // similarity Estimations Overview

    // minHash similarity estimation
    val simModel = new JaccardModel()
      .setInputCol("vectorizedFeatures")
      /* .setNumHashTables(10)
      .setInputCol("vectorizedFeatures")
      .setOutputCol("hashedFeatures")
      .fit(countVectorizedFeaturesDataFrame) */
    val simDF: Dataset[Row] = simModel
      .similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, valueColumn = "distCol") // , 0.9, "sim")
      .filter(col("uriA").notEqual(col("uriB")))
      .toDF()
    simDF
      .show(false)

    println("PROMISING CANDDATES")
    /* simDF
      .sort("uriA", "distCol")
      .show(false) */

    val tmpSchema = new StructType()
      .add(StructField("id", StringType, true))

    val candidatesForFE = spark.createDataFrame(
      simDF
        .rdd
        .flatMap(r => Seq(r(0).toString, r(1).toString))
        .distinct
        .map(Row(_)),
      tmpSchema
    )
      // .collect

    val candidatePairsForSimEst = simDF
      .select("uriA", "uriB")

    candidatesForFE
      .show(false)

    println("postfilter already filtered KG")
    val candidatesKG: Dataset[Triple] = candidatesForFE
      .rdd
      .map(r => Tuple2(r(0).toString, r(0)))
      .join(filtered.rdd.map(t => Tuple2(t.getSubject.toString(), t)))
      .map(_._2._2)
      .toDS()
      .as[Triple]

    candidatesKG
      .take(20)
      .foreach(println(_))

    println(candidatesKG.count())

    val triplesDfCan: DataFrame = candidatesKG
      .rdd
      .toDF()

    println("SmartFeatureExtractor")
    val sfe = new SmartFeatureExtractor()
      .setEntityColumnName("s")
    // sfe.transform()

    val feDf = sfe
      .transform(triplesDfCan)
    feDf
      .show(false)

    feDf
      .printSchema()

    println("Decision for SimilarityEstimationApproach")

    /* val similarityStrategies = feDf.schema.map(
      c => {
        if (c.name != "s") {
          val simMode = c.dataType match {
            case DoubleType => Tuple2(c.name, simMode)
            case StringType => Tuple2(c.name, simMode)
            case ArrayType(StringType, true) => Tuple2(c.name, simMode)
          }
        }
      }
    ) */

    var similarityExecutionOrder: Array[String] = null

    if (similarityExecutionOrder == null) similarityExecutionOrder = feDf
      .columns
      .drop(1) // drop first element cause it corresponds to entity column

    for (feature <- similarityExecutionOrder) {
      println("similarity estimation for feature: " + feature)
    }

    println()

    var similarityEstimations: DataFrame = simDF

    // similarityStrategies.foreach(println(_))

    similarityExecutionOrder.foreach(
      featureName => {
        print(featureName)
        // candidatePairsForSimEst.show()
        // feDf.show()

        val twoColFeDf = feDf.select("s", featureName)

        println("respective to feature type we need to normalize and change data so similarity estimator can operate on it")
        val featureDfNormalized = {
          if (twoColFeDf.schema(1).dataType == DoubleType) {

            val min_max = twoColFeDf.agg(min(featureName), max(featureName)).head()
            val col_min = min_max.getDouble(0)
            val col_max = min_max.getDouble(1)
            val range = if ((col_max - col_min) > 0) col_max - col_min else 1

            val myScaledData = twoColFeDf.withColumn("preparedFeature", (col(featureName) - lit(col_min)) / lit(range))
            myScaledData.show()
          }
          else if (twoColFeDf.schema(1).dataType == TimestampType) {

            val unixTimeStampDf = twoColFeDf.withColumn("unixTimestamp", unix_timestamp(col(featureName)))

            val min_max = unixTimeStampDf.agg(min("unixTimestamp"), max("unixTimestamp")).head()
            val col_min = min_max.getDouble(0)
            val col_max = min_max.getDouble(1)
            val range = if ((col_max - col_min) > 0) col_max - col_min else 1

            val myScaledData = twoColFeDf.withColumn("preparedFeature", (col(featureName) - lit(col_min)) / lit(range))
            myScaledData.show()
          }
          else twoColFeDf
        }

        /* val scaler = new StandardScaler()
          .setInputCol(featureName)
          .setOutputCol("scaled_" + featureName)
          .setWithStd(true)
          .setWithMean(false)

        // Compute summary statistics by fitting the StandardScaler.
        val scalerModel = scaler.fit(twoColFeDf)

        // Normalize each feature to have unit standard deviation.
        val scaledData = scalerModel.transform(twoColFeDf)
        scaledData.show()
        val scaler = new MinMaxScaler()
          .setInputCol(featureName)
          .setOutputCol("scaled_" + featureName)

        // Compute summary statistics and generate MinMaxScalerModel
        val scalerModel = scaler.fit(twoColFeDf.groupBy("s").agg(collect_list(featureName).as(featureName)))

        // rescale each feature to range [min, max].
        val scaledData = scalerModel.transform(twoColFeDf)

        scaledData.show(false) */

        val DfPairWithFeature = candidatePairsForSimEst
          .join(
            feDf.select("s", featureName).withColumnRenamed(featureName, featureName + "_uriA"),
            candidatePairsForSimEst("uriA") === feDf("s"),
          "inner")
          .drop("s")
          .join(
            feDf.select("s", featureName).withColumnRenamed(featureName, featureName + "_uriB"),
            candidatePairsForSimEst("uriB") === feDf("s"),
            "left")
          .drop("s")

        println("this is our combined dataframe for the respective feature: " + featureName)



        DfPairWithFeature.show(false)
      }
    )
  }

  /**
   * a method to calculate similarities for df with double features
   * @param df
   * @param featureColumn
   * @param uriAcolName
   * @param uriBcolName
   * @return
   */
  def doubleSim(df: DataFrame, featureColumn: String, uriAcolName: String = "uriA", uriBcolName: String = "uriB"): DataFrame = {
    df
  }
}
