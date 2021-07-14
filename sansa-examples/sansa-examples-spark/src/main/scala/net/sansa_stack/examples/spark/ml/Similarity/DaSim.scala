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
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
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
      .filter(t => ((t.getObject.toString().equals("http://dbpedia.org/ontology/Person"))))
      .limit(10)
      .rdd
      .toDF()

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

    println("the filtered kg has #triles:" + filtered.count())

    val triplesDf = filtered
      .rdd
      .toDF()

    // println("filtered KG")
    // triplesDf.show(false)

    val featureExtractorModel = new FeatureExtractorModel()
      .setMode("or")
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



    /*

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

 */



  }
}
