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
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Pilot3a {
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
    val dataset = spark.rdf(Lang.TTL)(inputPath).toDS()

    // dataset.rdd.coalesce(1).saveAsNTriplesFile(args(0).replace(".", " ") + "clean.nt")
    println(f"\ndata consists of ${dataset.count()} triples")
    dataset.take(n = 10).foreach(println(_))

    println("\nFEATURE EXTRACTION OVER SPARQL")
    /**
     * The sparql query used to gather features
     */
    val sparql = """
      SELECT
         ?building
         ?zone
         ?occupancyValue
         ?evalTime
         ?zoneLabel

         WHERE{
         ?building a <https://w3id.org/bot#Building> .

         OPTIONAL{
         ?building <https://w3id.org/bot#containsZone> ?zone .
         ?zone <http://www.w3.org/2000/01/rdf-schema#label> ?zoneLabel .}

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

    val sparqlFrame = new SparqlFrame()
      .setSparqlQuery(sparql)
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
    // extractedFeaturesDf.schema.foreach(println(_))

    println("\nSMART VECTOR ASSEMBLER")
    val smartVectorAssembler = new SmartVectorAssembler()
      .setEntityColumn("building")
      .setLabelColumn("occupancyValue(Single_NonCategorical_Decimal)")
    val assembledDf = smartVectorAssembler
      .transform(extractedFeaturesDf)
      .cache()

    assembledDf.show(10, false)


    println("\nAPPLY Common SPARK MLlib Example Algorithm")
    /**
     * drop rows where label is null
     */
    // Train a RandomForest model.
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")

    // Train model. This also runs the indexer.
    val model = rf.fit(assembledDf)

    // Make predictions.
    val predictions = model.transform(assembledDf)

    // Select example rows to display.
    predictions
      .select("entityID", "prediction", "label", "features")
      .show()

    val ml2Graph = new ML2Graph()
      .setEntityColumn("entityID")
      .setValueColumn("prediction")

    val metagraph: RDD[Triple] = ml2Graph.transform(predictions)
    metagraph.take(10).foreach(println(_))

    metagraph
      .coalesce(1)
      .saveAsNTriplesFile(args(0) + "someFolder")
  }
}
