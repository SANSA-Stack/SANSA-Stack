package net.sansa_stack.ml.spark.similarity.examples

import net.sansa_stack.ml.spark.similarity.similarityEstimationModels._
import net.sansa_stack.ml.spark.utils.{FeatureExtractorModel, SimilarityExperimentMetaGraphFactory}
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * This class object presents the minimal option to call available similarity functions
 * we read in our data over SANSA RDF
 * We extract the Features over FeatureExtractorModel
 * The extracted String based features are transformed to a index representation by Spark MLlib Count Vectorizer
 * then we perform the different similarity or distance models
 * always one for nearest neighbors
 * one for all pair similarity
 * all intermediate steps are printed in cmd line
 */
object MinimalCalls {
  def main(args: Array[String]): Unit = {

    // setup spark session
    val spark = SparkSession.builder
      .appName(s"MinMal Semantic Similarity Estimation Calls")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    // cause of jena NPE issue TODO ask Claus what is solution
    JenaSystem.init()

    // define inputpath if it is not parameter
    val inputPath = "./sansa-ml/sansa-ml-spark/src/main/resources/movieData/movie.nt"

    // read in data as Data`Frame
    val triplesDf: DataFrame = spark.read.rdf(Lang.NTRIPLES)(inputPath).cache()

    triplesDf.show(false)

    // feature extraction
    val featureExtractorModel = new FeatureExtractorModel()
      .setMode("an")
    val extractedFeaturesDataFrame = featureExtractorModel
      .transform(triplesDf)
      .filter(t => t.getAs[String]("uri").startsWith("m"))
      .cache()
    extractedFeaturesDataFrame.show(false)

    // filter for relevant URIs e.g. only movies
    val filteredFeaturesDataFrame = extractedFeaturesDataFrame.filter(t => t.getAs[String]("uri").startsWith("m")).cache()
    filteredFeaturesDataFrame.show(false)

    // count Vectorization
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("extractedFeatures")
      .setOutputCol("vectorizedFeatures")
      .fit(filteredFeaturesDataFrame)
    val tmpCvDf: DataFrame = cvModel.transform(filteredFeaturesDataFrame)
    // val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 }, DataTypes.BooleanType)
    val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 })
    val countVectorizedFeaturesDataFrame: DataFrame = tmpCvDf.filter(isNoneZeroVector(col("vectorizedFeatures"))).select("uri", "vectorizedFeatures").cache()
    countVectorizedFeaturesDataFrame.show(false)

    // similarity Estimations Overview
    // for nearestNeighbors we need one key which is a Vector to search for NN
    // val sample_key: Vector = countVectorizedFeaturesDataFrame.take(1)(0).getAs[Vector]("vectorizedFeatures")
    val sampleUri: String = "m2"
    val sample_key: Vector = countVectorizedFeaturesDataFrame
      .filter(countVectorizedFeaturesDataFrame("uri") === sampleUri)
      .take(1)(0)
      .getAs[Vector]("vectorizedFeatures")


    // minHash similarity estimation
    val minHashModel: MinHashModel = new MinHashModel()
      .setInputCol("vectorizedFeatures") /* new MinHashLSH()
      .setInputCol("vectorizedFeatures")
      .setOutputCol("hashedFeatures")
      .fit(countVectorizedFeaturesDataFrame) */
    minHashModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10, keyUri = sampleUri).show()
    minHashModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, 0.8, "distance").show()

    // Jaccard similarity
    val jaccardModel: JaccardModel = new JaccardModel()
     .setInputCol("vectorizedFeatures")
    jaccardModel
      .nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10, keyUri = sampleUri).show()
    jaccardModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).show()

    // Batet Distance
    val batetModel: BatetModel = new BatetModel()
      .setInputCol("vectorizedFeatures")
    batetModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10, keyUri = sampleUri).show()
    batetModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).show()

    // Braun Blanquet Similarity
    val braunBlanquetModel: BraunBlanquetModel = new BraunBlanquetModel()
      .setInputCol("vectorizedFeatures")
    braunBlanquetModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10, keyUri = sampleUri).show()
    braunBlanquetModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).show()

    // Dice Similarity
    val diceModel: DiceModel = new DiceModel()
      .setInputCol("vectorizedFeatures")
    diceModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10, keyUri = sampleUri).show()
    diceModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).show()

    // Ochiai Similarity
    val ochiaiModel: OchiaiModel = new OchiaiModel()
      .setInputCol("vectorizedFeatures")
    ochiaiModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10, keyUri = sampleUri).show()
    ochiaiModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).show()

    // Simpson Similarity
    val simpsonModel: SimpsonModel = new SimpsonModel()
      .setInputCol("vectorizedFeatures")
    simpsonModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10, keyUri = sampleUri).show()
    simpsonModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).show()

    // Tversky Similarity
    val tverskyModel: TverskyModel = new TverskyModel()
      .setInputCol("vectorizedFeatures")
      .setAlpha(1.0)
      .setBeta(1.0)
    tverskyModel.nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10, keyUri = sampleUri).show()
    tverskyModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, threshold = 0.5).show()


    // create Metagraph information for e.g. MinHash
    val model = new MinHashModel()  // : JaccardModel = new JaccardModel()
      .setInputCol("vectorizedFeatures")
    val outputDf1: Dataset[_] = model
      .nearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10, keyUri = sampleUri)

    val metaGraphFactory = new SimilarityExperimentMetaGraphFactory()
    val metagraph: RDD[graph.Triple] = metaGraphFactory.createRdfOutput(
      outputDataset = outputDf1)(
      modelInformationEstimatorName = model.estimatorName, modelInformationEstimatorType = model.modelType, modelInformationMeasurementType = model.estimatorMeasureType)(
      inputDatasetNumbertOfTriples = triplesDf.count(), dataSetInformationFilePath = inputPath)
    metagraph.foreach(println(_))

    /*
    // Similarity Estimation on Subset (e.g. Movie)
    // but maybe for sure we might only search for movie similaritties. so we would reduce dataframe as input
    // first we take only all movie uris
    val tmpDf = countVectorizedFeaturesDataFrame.filter(col("uri").startsWith("m"))
    // then we select one key of type movie
    val tmpK = tmpDf.take(1)(0).getAs[Vector]("vectorizedFeatures")
    minHashModel.approxNearestNeighbors(tmpDf, tmpK, 10, "minHashDistance").show()
    minHashModel.approxSimilarityJoin(tmpDf, tmpDf, 0.8, "distance").show()

    // Stacking of Approaches (MinHash + Jaccard)
    // this is interesting if we want to have precise resulting similarities but a more scalable approach.
    // for this purpose we first calculate similarity over the scalable minhash
    // and then we use the calculated candidates as filter for the initial dataset to reduce once again the dataframe.
    // then we calculate by common jaccard approach or others
    val minHashedSimilarities = minHashModel
      .approxSimilarityJoin(tmpDf, tmpDf, 0.8, "distance")
      .withColumn("uriA", col("datasetA").getField("uri"))
      .withColumn("uriB", col("datasetB").getField("uri"))
      .select("uriA", "uriB", "distance")
    val uriCandidates = (minHashedSimilarities.select("uriA").rdd.map(r => r(0)).collect().toSet.union(minHashedSimilarities.select("uriB").rdd.map(r => r(0)).collect().toSet)).toList
    val dfGoodCandidatesDf = tmpDf.filter(col("uri").isInCollection(uriCandidates))
    println(countVectorizedFeaturesDataFrame.count(), dfGoodCandidatesDf.count())
    jaccardModel.similarityJoin(dfGoodCandidatesDf, dfGoodCandidatesDf, threshold = 0.5).show()
    */
  }
}
