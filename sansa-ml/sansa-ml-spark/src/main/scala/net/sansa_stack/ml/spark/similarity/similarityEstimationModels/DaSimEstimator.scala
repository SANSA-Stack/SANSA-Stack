package net.sansa_stack.ml.spark.similarity.similarityEstimationModels

import java.util.{Calendar, Date}

import org.apache.spark.broadcast.Broadcast
import net.sansa_stack.ml.spark.featureExtraction.{SmartFeatureExtractor, SparqlFrame}
import net.sansa_stack.ml.spark.utils.FeatureExtractorModel
import net.sansa_stack.rdf.spark.io.RDFReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, HashingTF, IDF, MinHashLSH}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{abs, aggregate, array, avg, coalesce, col, collect_list, explode, first, lit, max, min, size, sort_array, struct, sum, udf, unix_timestamp, when}
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructField, StructType, TimestampType}

import scala.collection.mutable

class DaSimEstimator {

  // parameter

  // initial filter
  var _pInitialFilterBySPARQL: String = null
  var _pInitialFilterByObject: String = null
  var _pInitialFilterByPredicate: String = null


  // DistSIm candidate gathering
  var _pDistSimFeatureExtractionMethod = "or"
  var _pDistSimThreshold = 1.0

  // feature extraction
  var pSparqlFeatureExtractionQuery = null

  // similarity calculation
  var pSimilarityCalculationExecutionOrder: Array[String] = null

  // calc availability based on feature DF
  var _calcAvailability = true

  // final value aggregation
  var pValueStreching: Boolean = true
  var pAvailability: Map[String, Double] = null
  var pReliability: Map[String, Double] = null
  var pImportance: Map[String, Double] = null

  // general
  var _parameterVerboseProcess = false

  // eval
  var _seedLimit: Int = -1

  // semantification
  var _sem_entityCols: Broadcast[Array[String]] = null
  var _sem_finalValCol: Broadcast[String] = null
  var _sem_similarityCols: Broadcast[Array[String]] = null
  var _sem_availability: Broadcast[Map[String, Double]] = null
  var _sem_reliability: Broadcast[Map[String, Double]] = null
  var _sem_importance: Broadcast[Map[String, Double]] = null
  var _sem_distSimFeatureExtractionMethod: Broadcast[String] = null
  var _sem_initialFilter: Broadcast[String] = null
  var _sem_featureExtractionMethod: Broadcast[String] = null

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
   * FIlter init KG by spo object
   * Filter the KG by the predicate of spo structure, so an alternative and faster compared to sparql
   * @param predicateFilter string representing the object for spo filter
   * @return adjusted transformer
   */
  def setPredicateFilter(predicateFilter: String): this.type = {
    _pInitialFilterByObject = predicateFilter
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

  def setLimitSeeds(seedLimit: Int): this.type = {
    _seedLimit = seedLimit
    this
  }

  def setVerbose(verbose: Boolean): this.type = {
    _parameterVerboseProcess = verbose
    this
  }

  /**
   * internal method that collects seeds by either sparql or object filter
   * @param ds dataset of triples representing input kg
   * @param sparqlFilter filter by sparql initial kg
   * @param objectFilter gilter init kg by spo object
   * @return dataframe with one column containing string representation of seed URIs
   */
  def gatherSeeds(ds: Dataset[Triple], sparqlFilter: String = null, objectFilter: String = null, predicateFilter: String = null): DataFrame = {

    val spark = SparkSession.builder.getOrCreate()

    val seeds: DataFrame = {
    if (objectFilter!= null || predicateFilter!= null) {
      val tmpF = {
        if (objectFilter!= null && predicateFilter!= null) ds.filter (t => ((t.getObject.toString ().equals (objectFilter) ) && (t.getPredicate.toString ().equals (predicateFilter) ) ) )
        else if (objectFilter != null && predicateFilter == null) ds.filter (t => ((t.getObject.toString ().equals (objectFilter) ) ) )
        else ds.filter (t => (t.getPredicate.toString ().equals (predicateFilter) ) )
      }
      tmpF
        // .filter (t => ((t.getObject.toString ().equals (objectFilter) ) ) )
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
  def gatherCandidatePairs(dataset: Dataset[Triple], seeds: DataFrame, _pDistSimFeatureExtractionMethod: String = "os", fastNotDistSim: Boolean = true, _pDistSimThreshold: Double = 0): DataFrame = {
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

    val candidates = if (fastNotDistSim) {
      if (_parameterVerboseProcess) println("Fast method to gather candidate pairs")

      // extractedFeaturesDataFrame.show(false)

      val hashingTF = new HashingTF()
        .setInputCol("extractedFeatures")
        .setOutputCol("vectorizedFeatures")

      // .setNumFeatures(20)
      val hashedDf = hashingTF
        .transform(extractedFeaturesDataFrame)
        .select("uri", "vectorizedFeatures")
        .cache()


      val mh = new MinHashLSH()
        .setNumHashTables(5)
        .setInputCol("vectorizedFeatures")
        .setOutputCol("hashes")

      val model = mh.fit(hashedDf)

      val tmpSim = model
        .approxSimilarityJoin(hashedDf, hashedDf, _pDistSimThreshold, "JaccardDistance")
        .cache()
      // .withColumn("uriA", first(col("datasetA")))
      // .withColumn("uriB", first(col("datasetB")))
      // .select("uriA", "uriB", "JaccardDistance")

      // tmpSim.show(false)

      val filteredTmpDf = tmpSim
        .withColumn("uriA", col("datasetA").getField("uri"))
        .withColumn("uriB", col("datasetB").getField("uri"))
        .filter(col("uriA").notEqual(col("uriB")))
        .withColumn("set", array(col("uriA"), col("uriB")))
        .withColumn("sortedSet", sort_array(col("set")))
        .select("uriA", "uriB", "set", "sortedSet", "JaccardDistance")
        .dropDuplicates("sortedSet")
        .select("uriA", "uriB", "JaccardDistance")
        .cache()

      filteredTmpDf
    } else {
      // minHash similarity estimation
      if (_parameterVerboseProcess) println("DistSim method to gather candidate pairs")

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
        .select("uri", "vectorizedFeatures")
        .cache()

      // countVectorizedFeaturesDataFrame.show(false)

      val simModel = new JaccardModel()
        .setInputCol("vectorizedFeatures")
      /* .setNumHashTables(10)
      .setOutputCol("hashedFeatures")
      .fit(countVectorizedFeaturesDataFrame) */
      val simDF: DataFrame = spark.createDataFrame(
        simModel
          .similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, valueColumn = "distCol", threshold = _pDistSimThreshold) // , 0.9, "sim")
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

    candidates
      .select("uriA", "uriB")
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
  def gatherFeatures(ds: Dataset[Triple], candidates: DataFrame, sparqlFeatureExtractionQuery: String = null, predicateFilter: String = "", objectFilter: String = ""): DataFrame = {
    val featureDf = {
      if (sparqlFeatureExtractionQuery != null) {
        if (_parameterVerboseProcess) println("DaSimEstimator: Feature Extraction by SparqlFrame")
        val sf = new SparqlFrame()
          .setSparqlQuery(sparqlFeatureExtractionQuery)
        val tmpDf = sf
          .transform(ds)
        tmpDf
      }
      else {
        if (_parameterVerboseProcess) println("DaSimEstimator: Feature Extraction by SmartFeatureExtractor")

        implicit val rdfTripleEncoder: Encoder[Triple] = org.apache.spark.sql.Encoders.kryo[Triple]
        import net.sansa_stack.rdf.spark.model.TripleOperations

        val seedList: Array[String] = candidates.collect().map(_.getAs[String](0))

        val filteredDS: Dataset[Triple] = ds
          .filter(r => seedList.contains(r.getSubject.toString()))
          .map(_.asInstanceOf[Triple])
          .rdd
          .toDS()
          .cache()

        /* val filteredDF: DataFrame = ds
          .rdd
          .toDF()
          .join(candidates.withColumnRenamed("id", "s"), "s")
          .cache()
         */

        val sfe = new SmartFeatureExtractor()
          .setEntityColumnName("s")
          .setPredicateFilter(predicateFilter)
          .setObjectFilter(objectFilter)
        val feDf = sfe
          .transform(filteredDS)
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

            val entityColName = twoColFeDf.schema(0).name

            val unixTimeStampDf = twoColFeDf.withColumn("unixTimestamp", unix_timestamp(col(featureName)).cast("double"))

            // unixTimeStampDf.show()
            // unixTimeStampDf.printSchema()

            val min_max = unixTimeStampDf.agg(min("unixTimestamp"), max("unixTimestamp")).head()
            // println(min_max)
            val col_min = min_max.getDouble(0)
            val col_max = min_max.getDouble(1)
            val range = if ((col_max - col_min) != 0) col_max - col_min else 1

            val myScaledData = unixTimeStampDf
              .withColumn("preparedFeature", (col("unixTimestamp") - lit(col_min)) / lit(range))
              .select(entityColName, "preparedFeature")

            myScaledData
          }
          else if (twoColFeDf.schema(1).dataType == ArrayType(DoubleType)) {
            // println("whohoo new mode")

            val entityColName = twoColFeDf.schema(0).name

            val tmpTwoColDf = twoColFeDf.select(col(entityColName), explode(col(featureName)).as(featureName))

            // twoColFeDf.show()
            // tmpTwoColDf.show()

            val min_max = tmpTwoColDf.agg(min(featureName), max(featureName)).head()
            val col_min = min_max.getDouble(0)
            val col_max = min_max.getDouble(1)
            val range = if ((col_max - col_min) > 0) col_max - col_min else 1

            val myScaledData = tmpTwoColDf
              // .withColumn("preparedFeature", (col(featureName) - lit(col_min)) / lit(range))
              .withColumn("scaled", (col(featureName) - lit(col_min)) / lit(range))
              .groupBy(entityColName)
              .agg(collect_list("scaled") as "preparedFeature")
              // .agg(
              //  avg("scaled").alias("preparedFeature"))
              .select(entityColName, "preparedFeature")

            // myScaledData.show()

            myScaledData
          }
          else if (twoColFeDf.schema(1).dataType == ArrayType(TimestampType)) {
            // println("whohoo new mode")

            val entityColName = twoColFeDf.schema(0).name

            val tmpTwoColDf = twoColFeDf
              .select(col(entityColName), explode(col(featureName)).as(featureName))
              .withColumn("unixTimestamp", unix_timestamp(col(featureName)).cast("double"))
              .select(entityColName, "unixTimestamp")
              .withColumnRenamed("unixTimestamp", featureName)
              .select(entityColName, featureName)

            // twoColFeDf.show()
            // tmpTwoColDf.show()

            // val unixTimeStampDf = twoColFeDf.withColumn("unixTimestamp", unix_timestamp(col(featureName)).cast("double"))

            // unixTimeStampDf.show()
            // unixTimeStampDf.printSchema()

            val min_max = tmpTwoColDf.agg(min(featureName), max(featureName)).head()
            val col_min = min_max.getDouble(0)
            val col_max = min_max.getDouble(1)
            val range = if ((col_max - col_min) > 0) col_max - col_min else 1

            val myScaledData = tmpTwoColDf
              // .withColumn("preparedFeature", (col(featureName) - lit(col_min)) / lit(range))
              .withColumn("scaled", (col(featureName) - lit(col_min)) / lit(range))
              .groupBy(entityColName)
              .agg(collect_list("scaled") as "preparedFeature")
              // .agg(
              //  avg("scaled").alias("preparedFeature"))
              .select(entityColName, "preparedFeature")

            // myScaledData.show()

            myScaledData
          }
          else if (twoColFeDf.schema(1).dataType == ArrayType(StringType)) {
            val hashingTF = new HashingTF()
              .setInputCol(featureName)
              .setOutputCol("rawFeatures")
            // .setNumFeatures(20)

            // println(featureName)

            // twoColFeDf.show(false)

            val featurizedData = hashingTF
              .transform(twoColFeDf.withColumn(featureName, coalesce(col(featureName), array())))
            // alternatively, CountVectorizer can also be used to get term frequency vectors

            // featurizedData.show(false)

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

        // println(featureName)
        // featureDfNormalized.show(false)

        val DfPairWithFeature = candidatePairsDataFrame
          .join(
            featureDfNormalized
              .select("s", "preparedFeature")
              .withColumnRenamed("preparedFeature", featureName + "_prepared_uriA"),
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
          // if (_parameterVerboseProcess) tmpDf.show(false)

          similarityEstimations = similarityEstimations
            .join(
              tmpDf.select("uriA", "uriB", featureName + "_sim"),
              Seq("uriA", "uriB"),
              // similarityEstimations("uriA") === tmpDf("uriA") && similarityEstimations("uriB") === tmpDf("uriB"),
              "inner"
            )
        }

        /**
         * single numeric feature overlap calculation
         */
        else if ((twoColFeDf.schema(1).dataType == TimestampType) || twoColFeDf.schema(1).dataType == DoubleType) {

          val tmpDf = DfPairWithFeature
            .withColumn(featureName + "_sim", lit(1.0) - abs(col(featureName + "_prepared_uriA") - col(featureName + "_prepared_uriB")))
          // .select("uriA", "uriB", featureName + "_sim")
          // if (_parameterVerboseProcess) tmpDf.show(false)

          similarityEstimations = similarityEstimations
            .join(
              tmpDf.select("uriA", "uriB", featureName + "_sim"),
              Seq("uriA", "uriB"),
              // similarityEstimations("uriA") === tmpDf("uriA") && similarityEstimations("uriB") === tmpDf("uriB"),
              "inner"
            )
        }

        /**
         * lists of numeric features overlap calculation
         */
        else if (/* (twoColFeDf.schema(1).dataType == ArrayType(TimestampType)) || */ twoColFeDf.schema(1).dataType == ArrayType(DoubleType)) {

          val tmpDf = DfPairWithFeature
            .withColumn("uASum", aggregate(col(featureName + "_prepared_uriA"), lit(0.0), (x, y) => (x + y)))
            .withColumn("uBSum", aggregate(col(featureName + "_prepared_uriB"), lit(0.0), (x, y) => (x + y)))
            .withColumn("uASize", size(col(featureName + "_prepared_uriA")))
            .withColumn("uBSize", size(col(featureName + "_prepared_uriB")))
            .withColumn(featureName + "_sim", abs((col("uASum") / col("uASize")) - (col("uBSum") / col("uBSize"))))

          // .select("uriA", "uriB", featureName + "_sim")
          // if (_parameterVerboseProcess) tmpDf.show(false)

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
    similarityEstimations.na.fill(0.0)
  }

  def calculateAvailability(extractedFeaturesDF: DataFrame): mutable.Map[String, Double] = {
    val cols = extractedFeaturesDF.columns
    val entityCol: String = cols(0)
    val featureCols: Array[String] = cols.drop(1)

    val extractedFeaturesDfSize = extractedFeaturesDF.count().toDouble

    var availabilityMap: mutable.Map[String, Double] = mutable.Map()

    for (co <- featureCols) {
      val av = extractedFeaturesDF
        .select(co)
        .filter(col(co).isNull)
        .count
        .toDouble
      availabilityMap += (co + "_sim" -> (extractedFeaturesDfSize - av)/extractedFeaturesDfSize)
    }

    if (_parameterVerboseProcess) println("availability weighting (automatically calculated): ", availabilityMap.mkString(", "))

    availabilityMap
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

    val allCols = simDf.columns
    val sim_columns = allCols.slice(2, allCols.size)

    if (_parameterVerboseProcess) println(sim_columns.mkString(", "))

    val epsilon = 0.01

    // if these parameters are not set we calculate them as equally distributed ones
    if (pAvailability == null) {
      pAvailability = sim_columns.map(c => (c -> 1.0/sim_columns.length)).toMap
      if (_parameterVerboseProcess) println("DaSimEstimator: availability parameter is not set so it is automatically equally distributed: " + pAvailability)
    }
    if (pImportance == null) {
      pImportance = sim_columns.map(c => (c -> 1.0/sim_columns.length)).toMap
      if (_parameterVerboseProcess) println("DaSimEstimator: importance parameter is not set so it is automatically equally distributed: " + pImportance)
    }
    if (pReliability == null) {
      pReliability = sim_columns.map(c => (c -> 1.0/sim_columns.length)).toMap
      if (_parameterVerboseProcess) println("DaSimEstimator: reliability parameter is not set so it is automatically equally distributed: " + pReliability)
    }

    if (sim_columns.toSet.diff(pImportance.map(_._1).toSet).size > 0) {
      val tmp = sim_columns.toSet.diff(pImportance.map(_._1).toSet).map(sc => (sc, 0.0))
      pImportance = pImportance ++ tmp
      if (_parameterVerboseProcess) println(s"only some columns got Importance values, all others (${tmp.mkString(", ")}) are now set to 0.0")
    }
    if (sim_columns.toSet.diff(pAvailability.map(_._1).toSet).size > 0) {
      val tmp = sim_columns.toSet.diff(pAvailability.map(_._1).toSet).map(sc => (sc, 0.0))
      pAvailability = pAvailability ++ tmp
      if (_parameterVerboseProcess) println(s"only some columns got Availability values, all others (${tmp.mkString(", ")}) are now set to 0.0")
    }
    if (sim_columns.toSet.diff(pReliability.map(_._1).toSet).size > 0) {
      val tmp = sim_columns.toSet.diff(pReliability.map(_._1).toSet).map(sc => (sc, 0.0))
      pReliability = pReliability ++ tmp
      if (_parameterVerboseProcess) println(s"only some columns got Importance values, all others (${tmp.mkString(", ")})  are now set to 0.0")
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
                ( // TODO maybe better aggregation of weighning values. maybe multiplication not sum
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

  def semantification(
      resultDf: DataFrame
      // entityCols: Array[String] = null,
      // finalValCol: String = null,
      // similarityCols: Array[String] = null,
      // availability: Map[String, Double] = null,
      // reliability: Map[String, Double] = null,
      // importance: Map[String, Double] = null,
      // distSimFeatureExtractionMethod: String = null,
      // initialFilter: String = null,
      // featureExtractionMethod: String = null
    ): RDD[Triple] = {

    val spark = SparkSession.builder.getOrCreate()

    // strings for URIs
    var _elementPropertyURIasString: String = "https://sansa-stack.net/sansaVocab/element"
    var _valuePropertyURIasString: String = "https://sansa-stack.net/sansaVocab/value"
    var _commentPropertyURIasString: String = "https://sansa-stack.net/sansaVocab/comment"
    var _predictionPropertyURIasString: String = "https://sansa-stack.net/sansaVocab/prediction"
    var _experimentTypePropertyURIasString: String = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

    var _experimentTypeURIasString: String = "https://sansa-stack.net/sansaVocab/experiment"

    val hyperparameterNodeP = NodeFactory.createURI("https://sansa-stack.net/sansaVocab/hyperparameter")
    // val typeNode = NodeFactory.createURI(_experimentTypePropertyURIasString)
    val nodeLabel = NodeFactory.createURI("https://rdfs/label")

    // create reused nodes
    val typeNodeP = NodeFactory.createURI(_experimentTypePropertyURIasString)

    val valueNodeP = NodeFactory.createURI(_valuePropertyURIasString)

    val elementPropertyNode: Node = NodeFactory.createURI(_elementPropertyURIasString)


    // create experiment node
    val metagraphDatetime: Date = Calendar.getInstance().getTime()
    val experimentHash: String = metagraphDatetime.toString.hashCode.toString
    val experimentNode: Node = NodeFactory.createURI(_experimentTypeURIasString + "/" + experimentHash)

    val experimentTypePropertyNode: Node = NodeFactory.createURI(_experimentTypePropertyURIasString)
    val experimentTypeNode: Node = NodeFactory.createURI(_experimentTypeURIasString)
    val predictionPropertyNode: Node = NodeFactory.createURI(_predictionPropertyURIasString)
    val valuePropertyURINode: Node = NodeFactory.createURI(_valuePropertyURIasString)

    // overall annotation
    // Create all inforamtion for this central node
    if (_parameterVerboseProcess) println("central node triples")
    val centralNodeTriples: RDD[Triple] = spark.sqlContext.sparkContext.parallelize(List(
      Triple.create(
        experimentNode,
        experimentTypePropertyNode,
        experimentTypeNode
      )
    ))

    if (_parameterVerboseProcess) centralNodeTriples foreach println

    // distsim feature extraction
    val hyperparameterInitialFilter = NodeFactory.createURI(_experimentTypeURIasString + "/" + experimentHash + "/hyperparameter/initialFilter")
    val hyperparameterDistSimFeatureExtractionNode = NodeFactory.createURI(_experimentTypeURIasString + "/" + experimentHash + "/hyperparameter/distSimFeatureExtraction")
    val hyperparameterFeatureExtractionStrategy = NodeFactory.createURI(_experimentTypeURIasString + "/" + experimentHash + "/hyperparameter/featureExtractionStrategy")
    val hyperparameterAvailability = NodeFactory.createURI(_experimentTypeURIasString + "/" + experimentHash + "/hyperparameter/availability")
    val hyperparameterReliability = NodeFactory.createURI(_experimentTypeURIasString + "/" + experimentHash + "/hyperparameter/reliability")
    val hyperparameterImportance = NodeFactory.createURI(_experimentTypeURIasString + "/" + experimentHash + "/hyperparameter/importance")

    // now hyperparameters
    if (_parameterVerboseProcess) println("hyperparameer semantification")
    val hyperparameterTriples: RDD[Triple] = spark.sqlContext.sparkContext.parallelize(List(
      // hyperparameterInitialFilter
      Triple.create(
        experimentNode,
        hyperparameterNodeP,
        hyperparameterInitialFilter
      ),
      Triple.create(
        hyperparameterInitialFilter,
        typeNodeP,
        hyperparameterNodeP
      ),
      Triple.create(
        hyperparameterInitialFilter,
        nodeLabel,
        NodeFactory.createLiteral("initial filter")
      ),
      Triple.create(
        hyperparameterInitialFilter,
        valueNodeP,
        NodeFactory.createLiteral(_sem_initialFilter.value)
      ),

      // distsim feature extraction
      Triple.create(
        experimentNode,
        hyperparameterNodeP,
        hyperparameterDistSimFeatureExtractionNode
      ),
      Triple.create(
        hyperparameterDistSimFeatureExtractionNode,
        typeNodeP,
        hyperparameterNodeP
      ),
      Triple.create(
        hyperparameterDistSimFeatureExtractionNode,
        nodeLabel,
        NodeFactory.createLiteral("DistSim feature extraction strategy")
      ),
      Triple.create(
        hyperparameterDistSimFeatureExtractionNode,
        valueNodeP,
        NodeFactory.createLiteral(_sem_distSimFeatureExtractionMethod.value)
      ),

      // hyperparameterFeatureExtractionStrategy
      Triple.create(
        experimentNode,
        hyperparameterNodeP,
        hyperparameterFeatureExtractionStrategy
      ),
      Triple.create(
        hyperparameterFeatureExtractionStrategy,
        typeNodeP,
        hyperparameterNodeP
      ),
      Triple.create(
        hyperparameterFeatureExtractionStrategy,
        nodeLabel,
        NodeFactory.createLiteral("feature extraction strategy")
      ),
      Triple.create(
        hyperparameterFeatureExtractionStrategy,
        valueNodeP,
        NodeFactory.createLiteral(_sem_featureExtractionMethod.value)
      ),
      // hyperparameterAvailability
      Triple.create(
        experimentNode,
        hyperparameterNodeP,
        hyperparameterAvailability
      ),
      Triple.create(
        hyperparameterAvailability,
        typeNodeP,
        hyperparameterNodeP
      ),
      Triple.create(
        hyperparameterAvailability,
        nodeLabel,
        NodeFactory.createLiteral("availability")
      ),
      Triple.create(
        hyperparameterAvailability,
        valueNodeP,
        NodeFactory.createLiteral(_sem_availability.value.map(m => m._1 + ": " + m._2.toString).mkString("; "))
      ),
      // hyperparameterReliability
      Triple.create(
        experimentNode,
        hyperparameterNodeP,
        hyperparameterReliability
      ),
      Triple.create(
        hyperparameterReliability,
        typeNodeP,
        hyperparameterNodeP
      ),
      Triple.create(
        hyperparameterReliability,
        nodeLabel,
        NodeFactory.createLiteral("reliability")
      ),
      Triple.create(
        hyperparameterReliability,
        valueNodeP,
        NodeFactory.createLiteral(_sem_reliability.value.map(m => m._1 + ": " + m._2.toString).mkString("; "))
      ),
      // hyperparameterImportance
      Triple.create(
        experimentNode,
        hyperparameterNodeP,
        hyperparameterImportance
      ),
      Triple.create(
        hyperparameterImportance,
        typeNodeP,
        hyperparameterNodeP
      ),
      Triple.create(
        hyperparameterImportance,
        nodeLabel,
        NodeFactory.createLiteral("importance")
      ),
      Triple.create(
        hyperparameterImportance,
        valueNodeP,
        NodeFactory.createLiteral(_sem_importance.value.map(m => m._1 + ": " + m._2.toString).mkString("; "))
      )
    ))

    // hyperparameterTriples foreach println

    // now semantic representation of dimilsrity results
    val tmp_finalValCol = _sem_finalValCol.value
    val tmp_entityCols = _sem_entityCols.value
    val tmp_similarityCols = _sem_similarityCols.value

      // resultDf.rdd.map(row => row.getAs[Double](tmp_finalValCol)).foreach(println(_))

      /* resultDf.rdd.flatMap(row => {
        val uriA: String = row.getAs[String](tmp_entityCols(0))
        val uriB: String = row.getAs[String](tmp_entityCols(1))
        Array(uriA, uriB)
      }).foreach(println(_)) */

      // println("ent cols:", _sem_entityCols.mkString(", "))
    if (_parameterVerboseProcess) println("semantification of similarity values")

    val semanticResult = resultDf.rdd.flatMap(row => {
      val uriA: String = row.getAs[String](tmp_entityCols(0))
      val uriB: String = row.getAs[String](tmp_entityCols(1))
      val overall_similarity_score: Double = row.getAs[Double](tmp_finalValCol)
      val simScores: Array[(String, Double)] = tmp_similarityCols
        .map(sc => (sc, row.getAs[Double](sc)))
      val bestSimScore: Double = simScores
        .sortBy(_._2)
        .last
        ._2
      val epsilon = 0.001
      val listMostRelevant: Array[(String, Double)] = simScores
        .filter(ss => (bestSimScore - ss._2) < epsilon)
      // println(uriA, uriB, overall_similarity_score, listMostRelevant.map(sc => sc._1 + ": " + sc._2.toString).mkString("; "))
      val entityNodes = Array(
        NodeFactory.createURI(uriA),
        NodeFactory.createURI(uriB)
      )
      val valueNode = NodeFactory.createLiteralByValue(overall_similarity_score, XSDDatatype.XSDdouble)
      val commentNodeP = NodeFactory.createURI(_commentPropertyURIasString)
      val mostRelevantNode = NodeFactory.createURI("most relevant:" + listMostRelevant.map(sc => sc._1 + ": " + sc._2.toString).mkString("; "))
      // now semantification
      val predictionNode: Node = NodeFactory.createURI(experimentHash + entityNodes.map(_.getURI).mkString("").hashCode)
      // entity nodes to prediction blank node
      val entityNodeTriples: Array[Triple] = entityNodes.map(
        entityNode =>
          Triple.create(
            predictionNode,
            elementPropertyNode,
            entityNode
          )
      )
      // prediction blank node to overall experiment
      val valueExperimentTriples: Array[Triple] = Array(
        Triple.create(
          experimentNode,
          predictionPropertyNode,
          predictionNode
        ),
        Triple.create(
          predictionNode,
          valuePropertyURINode,
          valueNode
        ),
        Triple.create(
          predictionNode,
          commentNodeP,
          mostRelevantNode
        )
      )
      entityNodeTriples ++ valueExperimentTriples
    }
    )

    // now we need to merge central node, hyperparamters and semantic result
    centralNodeTriples.union(semanticResult).union(hyperparameterTriples)
  }

  /**
   * transforms da kg to a similarity score dataframe based on parameters
   * overall method encapsulating the methods and should be used from outside
   * @param dataset knowledge graph
   * @return dataframw with results of similarity scores as metagraph
   */
  def transform(dataset: Dataset[Triple]): DataFrame = {
    // gather seeds
    if (_parameterVerboseProcess) println("gather seeds")
    val seeds: DataFrame = if (_seedLimit == -1) {
      gatherSeeds(dataset, sparqlFilter = _pInitialFilterBySPARQL, predicateFilter = _pInitialFilterByPredicate, objectFilter = _pInitialFilterByObject)
        .cache()
    }
    else {
      gatherSeeds(dataset, sparqlFilter = _pInitialFilterBySPARQL, predicateFilter = _pInitialFilterByPredicate, objectFilter = _pInitialFilterByObject)
        .limit(_seedLimit) // TODO only tmp for debug and first try outs
        .cache()
    }
    if (_parameterVerboseProcess) seeds.show()
    // gather cadidate pairs by DistSim
    if (_parameterVerboseProcess) println("gather candidate pairs")
    val candidatePairs: DataFrame = gatherCandidatePairs(dataset, seeds, _pDistSimFeatureExtractionMethod, _pDistSimThreshold = _pDistSimThreshold)
      // .limit(100)
      .cache()

    if (_parameterVerboseProcess) println(s"We have ${candidatePairs.count()} candidate pairs")

    if (_parameterVerboseProcess) candidatePairs.show()
    // unique candidates
    if (_parameterVerboseProcess) println("unique candidates")
    val candidateList = listDistinctCandidates(candidatePairs).cache()
    if (_parameterVerboseProcess) candidateList.show()
    // feature extraction
    if (_parameterVerboseProcess) println("feature extraction")
    val featureDf: DataFrame = gatherFeatures(
      dataset,
      candidateList,
      sparqlFeatureExtractionQuery = if (pSparqlFeatureExtractionQuery != null) pSparqlFeatureExtractionQuery else null)
      .cache()
    if (_parameterVerboseProcess) featureDf.show()

    if (_parameterVerboseProcess) println(s"We have ${featureDf.count()} entries in feature DF pairs")


    // dasim similarity estimation calculation
    if (_parameterVerboseProcess) println("column wise similarity calculation")
    val similarityEstimations: DataFrame = calculateDaSimSimilarities(
      candidatePairs,
      featureDf
    )
      .cache()

    if (_parameterVerboseProcess) similarityEstimations.show()

    if (_parameterVerboseProcess) println("(optional) sim norm columns")
    val aggregatableDf = if (pValueStreching) normSimColumns(similarityEstimations) else similarityEstimations

    if (_calcAvailability) {
      val av = calculateAvailability(featureDf)
      pAvailability = av.toMap
    }

    if (_parameterVerboseProcess) println("final similarity aggregation")
    val aggregatedSimilarityScoreDf: DataFrame = aggregateSimilarityScore(
      aggregatableDf,
      pValueStreching,
      pAvailability,
      pImportance,
      pReliability
    )
    if (_parameterVerboseProcess) aggregatedSimilarityScoreDf.show()

    val spark = SparkSession.builder().getOrCreate()

    _sem_entityCols = spark.sparkContext.broadcast(aggregatedSimilarityScoreDf.columns.slice(0, 2))
    _sem_finalValCol = spark.sparkContext.broadcast("overall_similarity_score")
    _sem_similarityCols = spark.sparkContext.broadcast(aggregatedSimilarityScoreDf.columns.slice(2, aggregatedSimilarityScoreDf.columns.length - 1))
    _sem_availability = spark.sparkContext.broadcast(pAvailability)
    _sem_reliability = spark.sparkContext.broadcast(pReliability)
    _sem_importance = spark.sparkContext.broadcast(pImportance)
    _sem_distSimFeatureExtractionMethod = spark.sparkContext.broadcast(_pDistSimFeatureExtractionMethod)
    _sem_initialFilter = spark.sparkContext.broadcast({if (_pInitialFilterByObject != null) _pInitialFilterByObject else if (_pInitialFilterBySPARQL != null) _pInitialFilterBySPARQL else "no initial filter"})
    _sem_featureExtractionMethod = spark.sparkContext.broadcast(if (pSparqlFeatureExtractionQuery != null) pSparqlFeatureExtractionQuery else "SmartFeatureExtractor")

    aggregatedSimilarityScoreDf
  }
}
