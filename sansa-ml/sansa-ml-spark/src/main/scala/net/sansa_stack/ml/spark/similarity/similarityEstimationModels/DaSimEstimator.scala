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
  val _pDistSimFeatureExtractionMethod = "or"
  val _pDistSimThreshold = 1.0

  // general
  val _parameterVerboseProcess = false


  def setSparqlFilter(sparqlFilter: String): this.type = {
    _pInitialFilterBySPARQL = sparqlFilter
    this
  }

  def setObjectFilter(objectFilter: String): this.type = {
    _pInitialFilterByObject = objectFilter
    this
  }

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

    // println("filtered KG")
    // triplesDf.show(false)

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

  def listDistinctCandidates(candidatePairs: DataFrame): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()

    val tmpSchema = new StructType()
      .add(StructField("id", StringType, true))

    val candidatesForFE = spark.createDataFrame(
      candidatePairs
        .rdd
        .flatMap(r => Seq(r(0).toString, r(1).toString))
        .distinct
        .map(Row(_)),
      tmpSchema
    )
    candidatesForFE
  }

  def calculateDasinSimilarities(
    candidatePairsDataFrame: DataFrame,
    extractedFeatureDataframe: DataFrame,
    similarityExecutionOrder: Array[String],
    availability: Map[String, Double] = null,
    reliability: Map[String, Double] = null,
    importance: Map[String, Double] = null,

  ): DataFrame = {

    var similarityEstimations: DataFrame = candidatePairsDataFrame

    similarityExecutionOrder.foreach(
      featureName => {
        println(featureName)

        val twoColFeDf = extractedFeatureDataframe.select("s", featureName)

        if (_parameterVerboseProcess) println("respective to feature type we need to normalize and change data so similarity estimator can operate on it")
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
    println("SIMILARITY DATAFRAME")
    similarityEstimations

  }

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
    val featureDf: DataFrame = gatherFeatures(dataset, candidateList, null).cache()
    featureDf.show(false)
    featureDf
    // dasim similarity estimation calculation
    val similarityEstimations: DataFrame = calculateDasinSimilarities(candidatePairs, featureDf, featureDf.columns)
    similarityEstimations.show(false)
    similarityEstimations
  }
}
