package net.sansa_stack.examples.spark.ml.platoon

import net.sansa_stack.ml.spark.featureExtraction.{SmartVectorAssembler, SparqlFrame}
import net.sansa_stack.ml.spark.utils.ML2Graph
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.{NTripleReader, RDFReader}
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.graph
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, unix_timestamp}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object PLATOON_3A_Regression {
  def main(args: Array[String]): Unit = {

    // readIn
    val inputPath: String = "/Users/carstendraschner/Cloud/sciebo-research/PLATOON/Pilot3a_SANSA_colab/data/extractBuildingKG.ttl" // args(0) // http://www.cs.toronto.edu/~oktie/linkedmdb/linkedmdb-18-05-2009-dump.nt

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
    val dataset = spark.rdf(Lang.TTL)(inputPath).toDS()
    println(f"\ndata consists of ${dataset.count()} triples")
    dataset.take(n = 10).foreach(println(_))

    println("\nFEATURE EXTRACTION OVER SPARQL")
    /**
     * The sparql query used to gather features
     */

    val sparqlQuery = """
      SELECT
      ?building
      ?zone
      ?occupancyValue
      # ?evalTime
      (<http://www.w3.org/2001/XMLSchema#string>(?evalTime) as ?evalTime_asString) # TODO workaround until datetime is supported
      ?zoneLabel

      WHERE{
      ?building a <https://w3id.org/bot#Building> .

      OPTIONAL{
      ?building <https://w3id.org/bot#containsZone> ?zone .
      ?zone <rdfs:label> ?zoneLabel .
      }

      OPTIONAL {
      ?building <https://w3id.org/bot#containsZone> ?zone .
      ?zone <https://w3id.org/platoon/hasOccupancy> ?oc .
      ?oc <https://w3id.org/seas/evaluation> ?eval .
      ?eval <https://w3id.org/seas/evaluatedSimpleValue> ?occupancyValue .}
      OPTIONAL {
      ?building <https://w3id.org/bot#containsZone> ?zone .
      ?zone <https://w3id.org/platoon/hasOccupancy> ?oc .
      ?oc <https://w3id.org/seas/evaluation> ?eval .
      ?eval <https://w3id.org/seas/hasTemporalContext> ?evalTemp .
      ?evalTemp <http://www.w3.org/2006/time#inXSDDateTime> ?evalTime .}
      }
      """



    /**
     * transformer that collect the features from the Dataset[Triple] to a common spark Dataframe
     * collapsed
     * by column movie
     */
    val sparqlFrame = new SparqlFrame()
      .setSparqlQuery(sparqlQuery)
      .setCollapsByKey(true)
      .setCollapsColumnName("building")

    /**
     * dataframe with resulting features
     * in this collapsed by the movie column
     */
    val extractedFeaturesDf = sparqlFrame
      .transform(dataset)
      .cache()

    /**
     * feature descriptions of the resulting collapsed dataframe
     */
    val featureDescriptions = sparqlFrame.getFeatureDescriptions()
    println(s"Feature decriptions are:\n${featureDescriptions.mkString(",\n")}")

    extractedFeaturesDf.show(10, false)

    extractedFeaturesDf.schema.foreach(println(_))

    println("FEATURE EXTRACTION POSTPROCESSING")
     /**
     * Here we adjust things in dataframe which do not fit to our expectations like:
     * the fact that the runtime is in rdf data not annotated to be a double
     * but easy castable
     */
    val postprocessedFeaturesDf: DataFrame = extractedFeaturesDf
      .withColumn("evalTime_asDatetime(Single_NonCategorical_String)", col("evalTime_asString(Single_NonCategorical_String)").cast("timestamp"))
      .withColumn("unix_timestamp(Single_NonCategorical_Decimal)", unix_timestamp(col("evalTime_asDatetime(Single_NonCategorical_String)")))
      .withColumn("evalTime_asInt(Single_NonCategorical_Int)", col("unix_timestamp(Single_NonCategorical_Decimal)").cast("int"))
      .drop("evalTime_asString(Single_NonCategorical_String)")
      .select(
        "building",
        "occupancyValue(Single_NonCategorical_Decimal)",
        "evalTime_asInt(Single_NonCategorical_Int)"
      )
    postprocessedFeaturesDf.show(10, false)
    postprocessedFeaturesDf.schema.foreach(println(_))

    // postprocessedFeaturesDf.withColumn("tmp", col("movie__down_runtime(ListOf_NonCategorical_Int)").getItem(0)).show(false)

    println("\nSMART VECTOR ASSEMBLER")
    val smartVectorAssembler = new SmartVectorAssembler()
      .setEntityColumn("building")
      .setLabelColumn("occupancyValue(Single_NonCategorical_Decimal)")
      .setFeatureColumns(List("evalTime_asInt(Single_NonCategorical_Int)"))
      // .setNullReplacement("string", "")
      // .setNullReplacement("digit", -1)
      // .setWord2VecSize(5)
      // .setWord2VecMinCount(1)
      // .setWord2vecTrainingDfSizeRatio(svaWord2vecTrainingDfSizeRatio)

    val assembledDf: DataFrame = smartVectorAssembler
     .transform(postprocessedFeaturesDf)
     .cache()

    assembledDf.show(10, false)


    println("\nAPPLY Common SPARK MLlib Example Algorithm")
    /**
     * drop rows where label is null
     */
    val mlDf = assembledDf
      .filter(col("label").isNotNull)
    mlDf.show(10, false)

    // From here on process is used based on SApache SPark MLlib samples: https://spark.apache.org/docs/latest/ml-classification-regression.html#random-forest-regression

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = mlDf.randomSplit(Array(0.7, 0.3))

    // Train a RandomForest model.
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")

    // Train model. This also runs the indexer.
    val model = rf.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(trainingData)

    // Select example rows to display.
    predictions.select("entityID", "prediction", "label", "features").show(10)
    predictions.show()

    val ml2Graph = new ML2Graph()
      .setEntityColumn("entityID")
      .setValueColumn("prediction")

    val metagraph: RDD[Triple] = ml2Graph.transform(predictions)
    metagraph.take(10).foreach(println(_))
  }
}
