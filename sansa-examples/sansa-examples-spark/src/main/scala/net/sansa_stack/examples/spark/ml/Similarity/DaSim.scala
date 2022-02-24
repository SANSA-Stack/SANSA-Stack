package net.sansa_stack.examples.spark.ml.Similarity

import java.util.{Calendar, Date}

import net.sansa_stack.ml.spark.featureExtraction.{FeatureExtractingSparqlGenerator, SmartVectorAssembler, SparqlFrame}
import net.sansa_stack.ml.spark.featureExtraction._
import net.sansa_stack.ml.spark.similarity.similarityEstimationModels.{JaccardModel, MinHashModel}
import net.sansa_stack.ml.spark.utils.{FeatureExtractorModel, ML2Graph}
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.{NTripleReader, RDFReader}
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.graph
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, HashingTF, IDF, MinMaxScaler, StandardScaler, VectorAssembler}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}


object DaSim {
  def main(args: Array[String]): Unit = {

    val _parameter_distSimFeatureExtractionMethod = "os"
    val _parameterVerboseProcess = true

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
      val lang = Lang.NTRIPLES
      originalDataRDD = spark.rdf(lang)(input).persist()
      /* originalDataRDD = NTripleReader
        .load(
          spark,
          input,
          stopOnBadTerm = ErrorParseMode.SKIP,
          stopOnWarnings = WarningParseMode.IGNORE
        ) */
    } else {
      val lang = Lang.TURTLE
      originalDataRDD = spark.rdf(lang)(input).persist()
    }
    val dataset: Dataset[graph.Triple] = originalDataRDD
      .toDS()
      .cache()

    if (_parameterVerboseProcess) println(f"\ndata consists of ${dataset.count()} triples")

    if (_parameterVerboseProcess) dataset.take(n = 10).foreach(println(_))

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
      // .limit(10)
      .rdd
      .toDF()
      .select("s")

    if (_parameterVerboseProcess) seeds.show(false)

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

    if (_parameterVerboseProcess) println("the filtered kg has #triples:" + filtered.count())

    val triplesDf = filtered
      .rdd
      .toDF()

    // println("filtered KG")
    // triplesDf.show(false)

    val featureExtractorModel = new FeatureExtractorModel()
      .setMode(_parameter_distSimFeatureExtractionMethod)
    val extractedFeaturesDataFrame = featureExtractorModel
      .transform(triplesDf)

    if (_parameterVerboseProcess) extractedFeaturesDataFrame.show(false)

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
    if (_parameterVerboseProcess) countVectorizedFeaturesDataFrame.show(false)

    // similarity Estimations Overview

    // minHash similarity estimation
    val simModel = new JaccardModel()
      .setInputCol("vectorizedFeatures")
      /* .setNumHashTables(10)
      .setInputCol("vectorizedFeatures")
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

    if (_parameterVerboseProcess) simDF.show(false)

    println("PROMISING CANDDATES")

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

    if (_parameterVerboseProcess) candidatesForFE.show(false)

    println("POSTFILTER KG")
    val candidatesKG: Dataset[Triple] = candidatesForFE
      .rdd
      .map(r => Tuple2(r(0).toString, r(0)))
      .join(filtered.rdd.map(t => Tuple2(t.getSubject.toString(), t)))
      .map(_._2._2)
      .toDS()
      .as[Triple]

    if (_parameterVerboseProcess) candidatesKG.take(20).foreach(println(_))

    if (_parameterVerboseProcess) println(candidatesKG.count())

    val triplesDfCan: DataFrame = candidatesKG
      .rdd
      .toDF()

    println("SMARTFEATUREEXTRACTOR")
    val sfe = new SmartFeatureExtractor()
      .setEntityColumnName("s")
    // sfe.transform()

    val feDf = sfe
      .transform(candidatesKG)
    if (_parameterVerboseProcess) feDf.show(false)

    if (_parameterVerboseProcess) feDf.printSchema()

    if (_parameterVerboseProcess) println("Decision for SimilarityEstimationApproach")

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

    if (_parameterVerboseProcess) {
      for (feature <- similarityExecutionOrder) {
        println("similarity estimation for feature: " + feature)
      }
    }

    var similarityEstimations: DataFrame = simDF

    if (_parameterVerboseProcess) similarityEstimations.show(false)

    // similarityStrategies.foreach(println(_))

    println("CALCULATE SIMILARITIES")
    similarityExecutionOrder.foreach(
      featureName => {
        println(featureName)
        // candidatePairsForSimEst.show()
        // feDf.show()

        val twoColFeDf = feDf.select("s", featureName)

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

        val DfPairWithFeature = candidatePairsForSimEst
          .join(
            featureDfNormalized.select("s", "preparedFeature").withColumnRenamed("preparedFeature", featureName + "_prepared_uriA"),
            candidatePairsForSimEst("uriA") === feDf("s"),
          "inner")
          .drop("s")
          .join(
            featureDfNormalized.select("s", "preparedFeature").withColumnRenamed("preparedFeature", featureName + "_prepared_uriB"),
            candidatePairsForSimEst("uriB") === feDf("s"),
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
    similarityEstimations.show()

    println("OPTIONAL SIMILARITY STRECHING")
    val parameter_noralize_similarity_columns = true

    var norm_sim_df: DataFrame = similarityEstimations.cache()

    if (parameter_noralize_similarity_columns) {
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
    }

    if (_parameterVerboseProcess) norm_sim_df.show(false)

    println("WEIGTHED SUM OVER SIMILARITY VALUES")
    // println("Now we calculate the weighted Sum")
    val weighted_sum_df: DataFrame = if (parameter_noralize_similarity_columns) norm_sim_df else similarityEstimations

    val sim_columns = norm_sim_df.columns.drop(3)

    if (_parameterVerboseProcess) println("we will weight by four elements: importance, availability, information content, reliability")
    val epsilon = 0.01

    // importance
    var parameter_importance: Map[String, Double] = null
    if (parameter_importance == null) {
      parameter_importance = sim_columns.map(c => (c -> 1.0/sim_columns.length)).toMap
    }

    if (_parameterVerboseProcess) println("parameter_importance", parameter_importance)
    assert(1.0 - parameter_importance.toSeq.map(_._2).sum.abs < epsilon)


    // reliability
    var parameter_reliability: Map[String, Double] = null
    if (parameter_reliability == null) {
      parameter_reliability = sim_columns.map(c => (c -> 1.0/sim_columns.length)).toMap
    }

    if (_parameterVerboseProcess) println("parameter_reliability", parameter_reliability)
    assert(1.0 - parameter_reliability.toSeq.map(_._2).sum.abs < epsilon)

    // availability
    var parameter_availability: Map[String, Double] = null
    if (parameter_availability == null) {
      parameter_availability = sim_columns.map(c => (c -> 1.0/sim_columns.length)).toMap
    }

    if (_parameterVerboseProcess) println("parameter_availability", parameter_availability)
    assert(1.0 - parameter_availability.toSeq.map(_._2).sum.abs < epsilon)

    // now we calculate weighted sum
    var final_calc_df = weighted_sum_df
    sim_columns.foreach(
      sim_col => {
        final_calc_df = final_calc_df
          .withColumn("tmp_" + sim_col, col(sim_col) * (lit(parameter_availability(sim_col)) + lit(parameter_availability(sim_col)) + lit(parameter_importance(sim_col)))/3.0)
          // .drop(sim_col)
          // .withColumnRenamed("tmp", sim_col)
      }
    )

    // final_calc_df.show(false)

    final_calc_df = final_calc_df
      .withColumn("overall_similarity_score", sim_columns.map(sc => "tmp_" + sc).map(col).reduce((c1, c2) => c1 + c2))
    sim_columns.map(sc => "tmp_" + sc).foreach(sc => final_calc_df = final_calc_df.drop(sc))
    if (_parameterVerboseProcess) final_calc_df.show(false)

    println("SEMANTIFICATION OF RESULTS")

    /* TODO semantification of overall information:
    *  TODO experiment hash with timestamp and type
    *  TODO feature respective strategy for similarity estimation
    *  TODO feature weighting
    *  TODO initial element filter: sparql based, or programming absed
    *  TODO DistSim feature extraction method: "or", ...
    *  TODO feature extraction method like SmartFeatureExtractor
    * */

    // TODO sim values and most significant factors

    /* val ml2graph = new ML2Graph()
      .setEntityColumns(Array("uriA", "uriB"))
      .setValueColumn("overall_similarity_score")

    ml2graph
      .transform(final_calc_df)
      .foreach(println(_))
     */

    val semanticResult: RDD[Triple] = dasimSemantification(
      resultDf = final_calc_df,
      entityCols = Array("uriA", "uriB"),
      finalValCol = "overall_similarity_score",
      similarityCols = sim_columns,
      availability = parameter_availability,
      reliability = parameter_reliability,
      importance = parameter_importance,
      distSimFeatureExtractionMethod = _parameter_distSimFeatureExtractionMethod,
      initialFilter = "unknown", // TODO
      featureExtractionMethod = "unknown") // TODO

    if (_parameterVerboseProcess) semanticResult foreach println
    if (_parameterVerboseProcess) println(semanticResult.count())

    semanticResult.coalesce(1).saveAsNTriplesFile("/Users/carstendraschner/Downloads/tmpDasimOutput2")
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

  def dasimSemantification(
    resultDf: DataFrame,
    entityCols: Array[String],
    finalValCol: String,
    similarityCols: Array[String],
    availability: Map[String, Double],
    reliability: Map[String, Double],
    importance: Map[String, Double],
    distSimFeatureExtractionMethod: String,
    initialFilter: String,
    featureExtractionMethod: String
                          ): RDD[Triple] = {

    val spark = SparkSession.builder.getOrCreate()

    // strings for URIs
    var _elementPropertyURIasString: String = "sansa-stack/sansaVocab/element"
    var _valuePropertyURIasString: String = "sansa-stack/sansaVocab/value"
    var _commentPropertyURIasString: String = "sansa-stack/sansaVocab/comment"
    var _predictionPropertyURIasString: String = "sansa-stack/sansaVocab/prediction"
    var _experimentTypePropertyURIasString: String = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

    var _experimentTypeURIasString: String = "sansa-stack/sansaVocab/experiment"

    val hyperparameterNodeP = NodeFactory.createURI("sansa-stack/sansaVocab/hyperparameter")
    // val typeNode = NodeFactory.createURI(_experimentTypePropertyURIasString)
    val nodeLabel = NodeFactory.createURI("rdfs/label")

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
    // println("central node triples")
    val centralNodeTriples: RDD[Triple] = spark.sqlContext.sparkContext.parallelize(List(
      Triple.create(
        experimentNode,
        experimentTypePropertyNode,
        experimentTypeNode
      )
    ))
    centralNodeTriples foreach println

    // distsim feature extraction
    val hyperparameterInitialFilter = NodeFactory.createURI(_experimentTypeURIasString + "/" + experimentHash + "/hyperparameter/initialFilter")
    val hyperparameterDistSimFeatureExtractionNode = NodeFactory.createURI(_experimentTypeURIasString + "/" + experimentHash + "/hyperparameter/distSimFeatureExtraction")
    val hyperparameterFeatureExtractionStrategy = NodeFactory.createURI(_experimentTypeURIasString + "/" + experimentHash + "/hyperparameter/featureExtractionStrategy")
    val hyperparameterAvailability = NodeFactory.createURI(_experimentTypeURIasString + "/" + experimentHash + "/hyperparameter/availability")
    val hyperparameterReliability = NodeFactory.createURI(_experimentTypeURIasString + "/" + experimentHash + "/hyperparameter/reliability")
    val hyperparameterImportance = NodeFactory.createURI(_experimentTypeURIasString + "/" + experimentHash + "/hyperparameter/importance")

    // now hyperparameters
    // println("hyperparameer semantification")
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
        NodeFactory.createLiteral(initialFilter)
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
        NodeFactory.createLiteral(distSimFeatureExtractionMethod)
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
        NodeFactory.createLiteral(featureExtractionMethod)
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
        NodeFactory.createLiteral(availability.map(m => m._1 + ": " + m._2.toString).mkString("; "))
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
        NodeFactory.createLiteral(reliability.map(m => m._1 + ": " + m._2.toString).mkString("; "))
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
        NodeFactory.createLiteral(importance.map(m => m._1 + ": " + m._2.toString).mkString("; "))
      )
    ))
    hyperparameterTriples foreach println


    // now semantic representation of dimilsrity results
    // println("semantification of similarity values")
    val semanticResult = resultDf.rdd.flatMap(row => {
      val uriA = row.getAs[String](entityCols(0))
      val uriB = row.getAs[String](entityCols(1))

      val overall_similarity_score = row.getAs[Double]("overall_similarity_score")

      // now we need to get most important factor

      val simScores = similarityCols
        .map(sc => (sc, row.getAs[Double](sc)))

      val bestSimScore = simScores
        .sortBy(_._2)
        .last
        ._2

      val epsilon = 0.001

      val listMostRelevant = simScores
        .filter(ss => (bestSimScore - ss._2) < epsilon)

      (uriA, uriB, overall_similarity_score, listMostRelevant.map(sc => sc._1 + ": " + sc._2.toString).mkString("; "))

      val entityNodes = Array(
        NodeFactory.createURI(uriA),
        NodeFactory.createURI(uriB)
      )

      val valueNode = NodeFactory.createLiteralByValue(overall_similarity_score, XSDDatatype.XSDdouble)

      val commentNodeP = NodeFactory.createLiteral(_commentPropertyURIasString)
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
        /* Triple.create(
          predictionNode,
          valuePropertyURINode,
          valueNode
        ), */
        Triple.create(
          predictionNode,
          commentNodeP,
          mostRelevantNode
        )
      )
      entityNodeTriples ++ valueExperimentTriples
    })

    // semanticResult foreach println

    // now we need to merge central node, hyperparamters and semantic result
    centralNodeTriples.union(semanticResult).union(hyperparameterTriples)
  }
}
