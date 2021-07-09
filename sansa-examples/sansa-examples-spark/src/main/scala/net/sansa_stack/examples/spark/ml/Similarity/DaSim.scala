package net.sansa_stack.examples.spark.ml.Similarity

import net.sansa_stack.ml.spark.featureExtraction.{FeatureExtractingSparqlGenerator, SmartVectorAssembler, SparqlFrame}
import net.sansa_stack.ml.spark.similarity.similarityEstimationModels.MinHashModel
import net.sansa_stack.ml.spark.utils.{FeatureExtractorModel, ML2Graph}
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.graph
import org.apache.jena.graph.Triple
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}

object DaSim {
  def main(args: Array[String]): Unit = {

    // readIn
    val inputPath: String = args(0) // http://www.cs.toronto.edu/~oktie/linkedmdb/linkedmdb-18-05-2009-dump.nt

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
    val dataset: Dataset[graph.Triple] = {
      NTripleReader.load(
        spark,
        inputPath,
        stopOnBadTerm = ErrorParseMode.SKIP,
        stopOnWarnings = WarningParseMode.IGNORE
      )
        .toDS()
        .cache()
    }
    // dataset.rdd.coalesce(1).saveAsNTriplesFile(args(0).replace(".", " ") + "clean.nt")
    println(f"\ndata consists of ${dataset.count()} triples")
    dataset.take(n = 10).foreach(println(_))

    println("FETCH SEEDS")

    val p_seed_fetching_sparql =
      """
        |SELECT ?seed
        |WHERE {
        |?seed <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> .
        |}
        |""".stripMargin

    val sf = new SparqlFrame()
      .setSparqlQuery(p_seed_fetching_sparql)

    val seeds = sf.transform(dataset).limit(100) // TODO this is temporary

    println("seeds2")
    val seeds2 = dataset.filter(t => ((t.getObject.toString().equals("http://data.linkedmdb.org/movie/film")) & (t.getPredicate.toString().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))))
    seeds2
      .rdd
      .toDF()
      .limit(100)
      .show(false)
    println("seeds1")
    seeds.show(false)
    println(seeds.count())

    println("GATHER CANDIDATE PAIRS")

    implicit val rdfTripleEncoder: Encoder[Triple] = org.apache.spark.sql.Encoders.kryo[Triple]

    import spark.implicits._

    val seedsList = seeds.select("seed").map(r => r.toString()).collect()

    val filtered2 = dataset
      .filter(t => seedsList.contains(t.getSubject))
    filtered2
      .take(100)
      .foreach(println(_))

    val filtered: Dataset[Triple] = seeds.rdd
      .map(r => Tuple2(r(0).toString, r(0)))
      .join(dataset.rdd.map(t => Tuple2(t.getSubject.toString(), t)))
      .map(_._2._2)
      .toDS()
      .as[Triple]
    filtered.take(20).foreach(println(_))
    println(filtered.count())

    val triplesDf = filtered.rdd.toDF()

    triplesDf.show(false)

    val featureExtractorModel = new FeatureExtractorModel()
      .setMode("os")
    val extractedFeaturesDataFrame = featureExtractorModel
      .transform(triplesDf)

    extractedFeaturesDataFrame.show(false)

    // count Vectorization
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("extractedFeatures")
      .setOutputCol("vectorizedFeatures")
      .fit(extractedFeaturesDataFrame)
    val tmpCvDf: DataFrame = cvModel.transform(extractedFeaturesDataFrame)
    // val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 }, DataTypes.BooleanType)
    val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 })
    val countVectorizedFeaturesDataFrame: DataFrame = tmpCvDf
      // .filter(isNoneZeroVector(col("vectorizedFeatures")))
      .select("uri", "vectorizedFeatures").cache()
    countVectorizedFeaturesDataFrame.show(false)

    // similarity Estimations Overview

    // minHash similarity estimation
    val minHashModel: MinHashModel = new MinHashModel()
      .setInputCol("vectorizedFeatures") /* new MinHashLSH()
      .setInputCol("vectorizedFeatures")
      .setOutputCol("hashedFeatures")
      .fit(countVectorizedFeaturesDataFrame) */
    val simDF = minHashModel
      .similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, 0.5, "distance")
      .filter(col("uriA").notEqual(col("uriB")))
    simDF.show(false)

    println("PROMISING CANDDATES")
    val candidatesPerElement = 3
    simDF.sort("uriA", "distance").show(false)
    //  .groupBy("uriA").

    println("LITERAL2FEATURE SPARQL QUERY")
    val seedVarName = "?seed"
    val whereClauseForSeed = "?seed <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film>"
    val maxUp = 0
    val maxDown = 1
    val numberSeeds = 5

    val (totalSparqlQuery: String, var_names: List[String]) = FeatureExtractingSparqlGenerator.createSparql(
      ds = dataset,
      seedVarName = seedVarName,
      seedWhereClause = whereClauseForSeed,
      maxUp = maxUp,
      maxDown = maxDown,
      numberSeeds = numberSeeds
    )
    println(totalSparqlQuery)

    println("SPARQL FRAME FEATURE EXTTRACTION")

    val sf2 = new SparqlFrame()
      .setSparqlQuery(totalSparqlQuery)
      .setCollapsByKey(true)
      .setCollapsColumnName("seed")

    val featureDf = sf2
      .transform(dataset)

    featureDf.show(false)

    println("PIVOT FEATURE EXTRACTOR")

    val pivotFeatureDF = filtered
      .rdd
      .toDF()
      .groupBy("s")
      .pivot("p")
      .agg(collect_list("o"))
      .limit(1000)
    pivotFeatureDF
      .show(false)









  }
}
