package net.sansa_stack.ml.spark.similarity.similarityEstimationModels

import net.sansa_stack.ml.spark.featureExtraction.{SmartFeatureExtractor, SparqlFrame}
import net.sansa_stack.ml.spark.utils.FeatureExtractorModel
import net.sansa_stack.rdf.spark.io.RDFReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, HashingTF, IDF}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions.{abs, col, collect_list, lit, max, min, udf, unix_timestamp}
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructField, StructType, TimestampType}

class DaSimEstimator {

  // parameter

  // initial filter
  var _pInitialFilterBySPARQL: String = null
  var _pInitialFilterByObject: String = null

  // DistSIm candidate gathering
  var _pDistSimFeatureExtractionMethod = "or"
  var _pDistSimThreshold = 1.0

  // similarity calculation
  var pSimilarityCalculationExecutionOrder: Array[String] = null

  // final value aggregation
  var pValueStreching: Boolean = true
  var pAvailability: Map[String, Double] = null
  var pReliability: Map[String, Double] = null
  var pImportance: Map[String, Double] = null

  // general
  var _parameterVerboseProcess = false

  /**
   * candidate filtering sparql
   * with this parameter you can reduce the list of of candidates by use use of a sparql query
   * @param sparqlFilter SPARQL filter applied ontop of input KG
   * @return adjusted transformer
   */
  def setSparqlFilter(sparqlFilter: String): this.type = {
    _pInitialFilterBySPARQL = sparqlFilter
    this
  }

  /**
   * FIlter init KG by spo object
   * Filter the KG by the object of spo structure, so an alternative and faster compared to sparql
   * @param objectFilter string representing the object for spo filter
   * @return adjusted transformer
   */
  def setObjectFilter(objectFilter: String): this.type = {
    _pInitialFilterByObject = objectFilter
    this
  }

  /**
   * DistSim feature extraction method
   * feature extracting method for first guesses via DistSim
   * @param distSimFeatureExtractionMethod DistSim feature Extraction Method
   * @return adjusted transformer
   */
  def setDistSimFeatureExtractionMethod(distSimFeatureExtractionMethod: String): this.type = {
    _pDistSimFeatureExtractionMethod = distSimFeatureExtractionMethod
    this
  }

  /**
   * DistSim Threshold min Similarity
   * This is the threshold for minimal similarity score being used within Distsim for promising canidates
   * @param distSimThreshold DistSim threshold min similarity score for prefilter candidate pairs
   * @return adjusted transformer
   */
  def setDistSimThreshold(distSimThreshold: Double): this.type = {
    _pDistSimThreshold = distSimThreshold
    this
  }

  /**
   * Execution order of similarity scores
   * here you can specify in which order the similarity values should be executed
   * @param similarityCalculationExecutionOrder
   * @return adjusted transformer
   */
  def setSimilarityCalculationExecutionOrder(similarityCalculationExecutionOrder: Array[String]): this.type = {
    pSimilarityCalculationExecutionOrder = similarityCalculationExecutionOrder
    this
  }

  /**
   * Normalize similairty scores per feature
   * this parameter offers that the feature dedicated similarity scores are streched/normed s.t. they all reach from zero to one
   * @param valueStreching
   * @return adjusted transformer
   */
  def setSimilarityValueStreching(valueStreching: Boolean): this.type = {
    pValueStreching = valueStreching
    this
  }

  /**
   * specify manually the availability of each feature
   * this parameter weights the relevance of a certain feature similarity based on their availability
   * it is possible that the availability is known
   * if the value is not given, it will be considered to be equally distributed
   * @param availability
   * @return adjusted transformer
   */
  def setAvailability(availability: Map[String, Double]): this.type = {
    pAvailability = availability
    this
  }

  /**
   * specify manually the reliability of each feature
   * this parameter weights the relevance of a certain feature similarity based on their reliability
   * it is possible that the reliability is known, for example that certain data might be influenced by ffake news or that data is rarely updated
   * if the value is not given, it will be considered to be equally distributed
   * @param reliability
   * @return adjusted transformer
   */
  def setReliability(reliability: Map[String, Double]): this.type = {
    pReliability = reliability
    this
  }

  /**
   * specify manually the importance of each feature
   * this parameter weights the relevance of a certain feature similarity based on their importance
   * this value offers user to influence weightning on personal preferance
   * @param importance
   * @return adjusted transformer
   */
  def setImportance(importance: Map[String, Double]): this.type = {
    pImportance = importance
    this
  }

  /**
   * internal method that collects seeds by either sparql or object filter
   * @param ds dataset of triples representing input kg
   * @param sparqlFilter filter by sparql initial kg
   * @param objectFilter gilter init kg by spo object
   * @return dataframe with one column containing string representation of seed URIs
   */
  def gatherSeeds(ds: Dataset[Triple], sparqlFilter: String = null, objectFilter: String = null): DataFrame = {

    val spark = SparkSession.builder.getOrCreate()

    val seeds: DataFrame = {
    if (objectFilter!= null) {
      ds
        .filter (t => ((t.getObject.toString ().equals (objectFilter) ) ) )
        .rdd
        .toDF()
        .select("s")
        .withColumnRenamed("s", "seed")
    }
    else if (sparqlFilter != null) {
      val sf = new SparqlFrame()
        .setSparqlQuery(sparqlFilter)
      val tmpDf = sf
        .transform(ds)
      val cn: Array[String] = tmpDf.columns
      tmpDf
        .withColumnRenamed(cn(0), "seed")
    }
    else {
      val tmpSchema = new StructType()
        .add(StructField("seed", StringType, true))

      spark.createDataFrame(
        ds
          .rdd
          .flatMap(t => Seq(t.getSubject, t.getObject))
          .filter(_.isURI)
          .map(_.toString())
          .distinct
          .map(Row(_)),
        tmpSchema
      )
    }
  }
    // assert(seeds.columns == Array("seed"))
  seeds
  }

  /**
   * we use distsim to gather promising candidates
   * @param dataset prefiltered KG for gathering candidates
   * @param seeds the seeds to be used for calculating promising cadidates via DistSim
   * @param _pDistSimFeatureExtractionMethod method for distsim feature extractor
   * @param _pDistSimThreshold threshold for distsim postfilter pairs by min threshold
   * @return dataframe with candidate pairs resulting from DistSim
   */
  def gatherCandidatePairs(dataset: Dataset[Triple], seeds: DataFrame, _pDistSimFeatureExtractionMethod: String = "os", _pDistSimThreshold: Double = 0): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    implicit val rdfTripleEncoder: Encoder[Triple] = org.apache.spark.sql.Encoders.kryo[Triple]

    val filtered: Dataset[Triple] = seeds
      .rdd
      .map(r => Tuple2(r(0).toString, r(0)))
      .join(dataset.rdd.map(t => Tuple2(t.getSubject.toString(), t)))
      .map(_._2._2)
      .toDS()
      .as[Triple]

    val triplesDf = filtered
      .rdd
      .toDF()

    val featureExtractorModel = new FeatureExtractorModel()
      .setMode(_pDistSimFeatureExtractionMethod)
    val extractedFeaturesDataFrame = featureExtractorModel
      .transform(triplesDf)

    // extractedFeaturesDataFrame.show(false)

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
    // similarity Estimations Overview

    // countVectorizedFeaturesDataFrame.show(false)

    // minHash similarity estimation
    val simModel = new JaccardModel()
      .setInputCol("vectorizedFeatures")
    /* .setNumHashTables(10)
    .setOutputCol("hashedFeatures")
    .fit(countVectorizedFeaturesDataFrame) */
    val simDF: DataFrame = spark.createDataFrame(
      simModel
        .similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, valueColumn = "distCol") // , 0.9, "sim")
        .filter(col("uriA").notEqual(col("uriB")))
        .rdd //  from here on drop symetric dublicates
        .map(r => Set(r.getString(0), r.getString(1)))
        .distinct()
        .map(s => s.toSeq)
        .map(l => Row(l(0), l(1))),
      new StructType()
        .add(StructField("uriA", StringType, true))
        .add(StructField("uriB", StringType, true))
    )


    val candidatePairsForSimEst = simDF
      .select("uriA", "uriB")

    candidatePairsForSimEst
  }

  /**
   * feature extraction for extensive similarity scores
   * creates dataframe with all features
   * two options for feature gathering
   * either SparqlFrame
   * or SmartFeature Extractor which operates pivot based
   * @param ds dataset of KG
   * @param candidates dandidate pairs from distsim
   * @param sparqlFeatureExtractionQuery optional, but if set we use sparql frame and not smartfeatureextractor
   * @return dataframe with columns corresponding to the features and the uri identifier
   */
  def gatherFeatures(ds: Dataset[Triple], candidates: DataFrame, sparqlFeatureExtractionQuery: String = null): DataFrame = {
    val featureDf = {
      if (sparqlFeatureExtractionQuery != null) {
        println("DaSimEstimator: Feature Extraction by SparqlFrame")
        val sf = new SparqlFrame()
          .setSparqlQuery(sparqlFeatureExtractionQuery)
        val tmpDf = sf
          .transform(ds)
        tmpDf
      }
      else {
        println("DaSimEstimator: Feature Extraction by SmartFeatureExtractor")

        implicit val rdfTripleEncoder: Encoder[Triple] = org.apache.spark.sql.Encoders.kryo[Triple]
        import net.sansa_stack.rdf.spark.model.TripleOperations

        val filteredDF: DataFrame = ds
          .rdd
          .toDF()
          .join(candidates.withColumnRenamed("id", "s"), "s")
          .cache()

        val sfe = new SmartFeatureExtractor()
          .setEntityColumnName("s")
        val feDf = sfe
          .transform(filteredDF)
        feDf
      }
    }
    featureDf
  }

  /**
   * list all elements which exists within the resulting uris of distsim
   * @param candidatePairs candidate pairs in a dataframe coming from distsim
   * @return dataframw ith one column having the relevant uris as strings
   */
  def listDistinctCandidates(candidatePairs: DataFrame): DataFrame = {

    candidatePairs
      .drop("uriB")
      .withColumnRenamed("uriA", "id")
      .union(
        candidatePairs
          .drop("uriA")
          .withColumnRenamed("uriB", "id")
      ).distinct()
  }

  /**
   * calculate with the new approach the weighted and feature specific simialrity scores
   *
   * @param candidatePairsDataFrame candidate pairs which span up the combinations to be calculated on
   * @param extractedFeatureDataframe extracted feature dataframe
   * @return calculate for each feature the pairwise similarity score
   */
  def calculateDaSimSimilarities(
    candidatePairsDataFrame: DataFrame,
    extractedFeatureDataframe: DataFrame,
  ): DataFrame = {

    var similarityEstimations: DataFrame = candidatePairsDataFrame

    if (pSimilarityCalculationExecutionOrder == null) pSimilarityCalculationExecutionOrder = extractedFeatureDataframe.columns.drop(1)

    pSimilarityCalculationExecutionOrder.foreach(
      featureName => {
        // println(featureName)

        val twoColFeDf = extractedFeatureDataframe.select("s", featureName)

        // if (_parameterVerboseProcess) println("respective to feature type we need to normalize and change data so similarity estimator can operate on it")
        val featureDfNormalized = {
          if (twoColFeDf.schema(1).dataType == DoubleType) {

            val min_max = twoColFeDf.agg(min(featureName), max(featureName)).head()
            val col_min = min_max.getDouble(0)
            val col_max = min_max.getDouble(1)
            val range = if ((col_max - col_min) > 0) col_max - col_min else 1

            val myScaledData = twoColFeDf.withColumn("preparedFeature", (col(featureName) - lit(col_min)) / lit(range))

            myScaledData
          }
          else if (twoColFeDf.schema(1).dataType == TimestampType) {

            val unixTimeStampDf = twoColFeDf.withColumn("unixTimestamp", unix_timestamp(col(featureName)).cast("double"))

            // unixTimeStampDf.show()
            // unixTimeStampDf.printSchema()

            val min_max = unixTimeStampDf.agg(min("unixTimestamp"), max("unixTimestamp")).head()
            // println(min_max)
            val col_min = min_max.getDouble(0)
            val col_max = min_max.getDouble(1)
            val range = if ((col_max - col_min) != 0) col_max - col_min else 1

            val myScaledData = unixTimeStampDf.withColumn("preparedFeature", (col("unixTimestamp") - lit(col_min)) / lit(range))

            myScaledData
          }
          else if (twoColFeDf.schema(1).dataType == ArrayType(StringType)) {
            val hashingTF = new HashingTF()
              .setInputCol(featureName)
              .setOutputCol("rawFeatures")
            // .setNumFeatures(20)

            val featurizedData = hashingTF
              .transform(twoColFeDf)
            // alternatively, CountVectorizer can also be used to get term frequency vectors

            val idf = new IDF()
              .setInputCol("rawFeatures")
              .setOutputCol("preparedFeature")
            val idfModel = idf
              .fit(featurizedData)

            val rescaledData = idfModel
              .transform(featurizedData)
            rescaledData
              .select("s", "preparedFeature")
          }
          else if (twoColFeDf.schema(1).dataType == StringType) {
            val twoColListFeDf = twoColFeDf
              .groupBy("s").agg(collect_list(featureName) as "tmp")
              .select("s", "tmp")

            val hashingTF = new HashingTF()
              .setInputCol("tmp")
              .setOutputCol("rawFeatures")
            // .setNumFeatures(20)

            val featurizedData = hashingTF
              .transform(twoColListFeDf)
            // alternatively, CountVectorizer can also be used to get term frequency vectors

            val idf = new IDF()
              .setInputCol("rawFeatures")
              .setOutputCol("preparedFeature")
            val idfModel = idf
              .fit(featurizedData)

            val rescaledData = idfModel
              .transform(featurizedData)
            rescaledData
              .select("s", "preparedFeature")

            /* twoColFeDf.withColumn("preparedFeature", hash(col(featureName)).cast("double"))
              .select("s", "preparedFeature") */
          }
          else {
            println("you should never end up here")


            twoColFeDf.withColumnRenamed(featureName, "preparedFeature")
          }
        }

        val DfPairWithFeature = candidatePairsDataFrame
          .join(
            featureDfNormalized.select("s", "preparedFeature").withColumnRenamed("preparedFeature", featureName + "_prepared_uriA"),
            candidatePairsDataFrame("uriA") === extractedFeatureDataframe("s"),
            "inner")
          .drop("s")
          .join(
            featureDfNormalized.select("s", "preparedFeature").withColumnRenamed("preparedFeature", featureName + "_prepared_uriB"),
            candidatePairsDataFrame("uriB") === extractedFeatureDataframe("s"),
            "left")
          .drop("s")

        if (_parameterVerboseProcess) println("this is our combined dataframe for the respective feature: " + featureName)



        // DfPairWithFeature.show(false)

        if (_parameterVerboseProcess) println("now we execute the respective similarity estimation for this df of candidates")

        if (_parameterVerboseProcess) println("we need to decide about similarity type by column data type")

        /**
         * categorical feature overlap calculation
         */
        if ((twoColFeDf.schema(1).dataType == StringType) || twoColFeDf.schema(1).dataType == ArrayType(StringType)) {
          val jaccard = udf( (a: Vector, b: Vector) => {
            // val featureIndicesA = a.toSparse.indices
            // val featureIndicesB = b.toSparseindices
            val fSetA = a.toSparse.indices.toSet
            val fSetB = b.toSparse.indices.toSet
            val intersection = fSetA.intersect(fSetB).size.toDouble
            val union = fSetA.union(fSetB).size.toDouble
            if (union == 0.0) {
              0
            }
            else {
              val jaccard = intersection / union
              jaccard
            }

          })

          val tmpDf = DfPairWithFeature
            .withColumn(featureName + "_sim", jaccard(col(featureName + "_prepared_uriA"), col(featureName + "_prepared_uriB")))
          // .select("uriA", "uriB", featureName + "_sim")
          if (_parameterVerboseProcess) tmpDf.show(false)

          similarityEstimations = similarityEstimations
            .join(
              tmpDf.select("uriA", "uriB", featureName + "_sim"),
              Seq("uriA", "uriB"),
              // similarityEstimations("uriA") === tmpDf("uriA") && similarityEstimations("uriB") === tmpDf("uriB"),
              "inner"
            )
        }

        /**
         * categorical feature overlap calculation
         */
        else if ((twoColFeDf.schema(1).dataType == TimestampType) || twoColFeDf.schema(1).dataType == DoubleType) {

          val tmpDf = DfPairWithFeature
            .withColumn(featureName + "_sim", lit(1.0) - abs(col(featureName + "_prepared_uriA") - col(featureName + "_prepared_uriB")))
          // .select("uriA", "uriB", featureName + "_sim")
          if (_parameterVerboseProcess) tmpDf.show(false)

          similarityEstimations = similarityEstimations
            .join(
              tmpDf.select("uriA", "uriB", featureName + "_sim"),
              Seq("uriA", "uriB"),
              // similarityEstimations("uriA") === tmpDf("uriA") && similarityEstimations("uriB") === tmpDf("uriB"),
              "inner"
            )
        }
      }
    )
    similarityEstimations

  }

  /**
   * optional method to normalize similarity columns
   * @param df similarity scored dataframe which needs to be normalized
   * @return normalized dataframe
   */
  def normSimColumns(df: DataFrame): DataFrame = {
    var norm_sim_df: DataFrame = df.cache()

    val sim_columns = norm_sim_df.columns.drop(3)

    sim_columns.foreach(
      sim_col => {
        val min_max = norm_sim_df.agg(min(sim_col), max(sim_col)).head()
        val col_min = min_max.getDouble(0)
        val col_max = min_max.getDouble(1)
        val range = if ((col_max - col_min) != 0) col_max - col_min else 1

        norm_sim_df = norm_sim_df
          .withColumn("tmp", (col(sim_col) - lit(col_min)) / lit(range))
          .drop(sim_col)
          .withColumnRenamed("tmp", sim_col)
      }
    )
    norm_sim_df
  }

  /**
   * aggregate similarity scores and weight those
   * @param simDf similarity dataframw with the feature specific sim scores
   * @param valueStreching parameter, optional to strech features, by deafault set
   * @param availability weightning by availability
   * @param importance user specific weighning over importance
   * @param reliability optional opportunity to incluence weighning by reliability
   * @return similarity dataframe with aggregated and weigthed final similarity score
   */
  def aggregateSimilarityScore(
    simDf: DataFrame,
    valueStreching: Boolean = true,
    availability: Map[String, Double] = null,
    importance: Map[String, Double] = null,
    reliability: Map[String, Double] = null
                              ): DataFrame = {

    val sim_columns = simDf.columns.drop(3)

    val epsilon = 0.01

    // if these parameters are not set we calculate them as equally distributed ones
    if (pAvailability == null) {
      pAvailability = sim_columns.map(c => (c -> 1.0/sim_columns.length)).toMap
      println("DaSimEstimator: availability parameter is not set so it is automatically equally distributed: " + pAvailability)
    }
    if (pImportance == null) {
      pImportance = sim_columns.map(c => (c -> 1.0/sim_columns.length)).toMap
      println("DaSimEstimator: importance parameter is not set so it is automatically equally distributed: " + pImportance)
    }
    if (pReliability == null) {
      pReliability = sim_columns.map(c => (c -> 1.0/sim_columns.length)).toMap
      println("DaSimEstimator: reliability parameter is not set so it is automatically equally distributed: " + pReliability)
    }

    // now we calculate weighted sum
    var final_calc_df = simDf
    sim_columns.foreach(
      sim_col => {
        final_calc_df = final_calc_df
          .withColumn(
            "tmp_" + sim_col,
            {
              col(sim_col) *
                (
                  lit(pAvailability(sim_col)) +
                  lit(pImportance(sim_col)) +
                  lit(pReliability(sim_col))
                  )/3.0
            })
        // .drop(sim_col)
        // .withColumnRenamed("tmp", sim_col)
      }
    )

    // final_calc_df
    final_calc_df = final_calc_df
      .withColumn("overall_similarity_score", sim_columns.map(sc => "tmp_" + sc).map(col).reduce((c1, c2) => c1 + c2))
    // drop helper columns
    sim_columns
      .map(sc => "tmp_" + sc)
      .foreach(sc => final_calc_df = final_calc_df.drop(sc))
    final_calc_df
  }



  /**
   * transforms da kg to a similarity score dataframe based on parameters
   * overall method encapsulating the methods and should be used from outside
   * @param dataset knowledge graph
   * @return dataframw with results of similarity scores as metagraph
   */
  def transform(dataset: Dataset[Triple]): DataFrame = {
    // gather seeds
    println("gather seeds")
    val seeds: DataFrame = gatherSeeds(dataset, _pInitialFilterBySPARQL, _pInitialFilterByObject).cache()
    seeds.show(false)
    // gather cadidate pairs by DistSim
    println("gather candidate pairs by DistSim")
    val candidatePairs: DataFrame = gatherCandidatePairs(dataset, seeds, _pDistSimFeatureExtractionMethod, _pDistSimThreshold).cache()
    candidatePairs.show(false)
    // unique candidates
    println("unique candidates")
    val candidateList = listDistinctCandidates(candidatePairs).cache()
    candidateList.show(false)
    // feature extraction
    println("feature extraction")
    val featureDf: DataFrame = gatherFeatures(
      dataset,
      candidateList,
      null).cache()
    featureDf.show(false)
    featureDf
    // dasim similarity estimation calculation
    println("column wise similarity calculation")
    val similarityEstimations: DataFrame = calculateDaSimSimilarities(
      candidatePairs,
      featureDf
    ).cache()
    similarityEstimations.show(false)

    println("(optional) sim norm columns")
    val aggregatableDf = if (pValueStreching) normSimColumns(similarityEstimations) else similarityEstimations

    println("final similarity aggregation")
    val aggregatedSimilarityScoreDf: DataFrame = aggregateSimilarityScore(
      aggregatableDf,
      pValueStreching,
      pAvailability,
      pImportance,
      pReliability
    )
    aggregatedSimilarityScoreDf.show(false)
    aggregatedSimilarityScoreDf
  }
}
