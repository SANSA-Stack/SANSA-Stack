package net.sansa_stack.examples.spark.ml

import net.sansa_stack.ml.spark.featureExtraction.{FeatureExtractingSparqlGenerator, SmartVectorAssembler, SparqlFrame}
import net.sansa_stack.rdf.spark.io.{RDFDataFrameReader, RDFReader}
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession}

import net.sansa_stack.query.spark.SPARQLEngine

object EAUC_Pipeline {
  def main(args: Array[String]): Unit = {
    // setup spark session
    val spark = SparkSession.builder
      .appName(s"SampleFeatureExtractionPipeline")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // we need Kryo serialization enabled with some custom serializers
      .config("spark.kryo.registrator", String.join(
        ", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"))
      // .config("spark.sql.crossJoin.enabled", true) // needs to be enabled if your SPARQL query does make use of cartesian product Note: in Spark 3.x it's enabled by default
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    JenaSystem.init()

    /*
    READ IN DATA
     */
    val inputFilePath = "/Users/carstendraschner/Cloud/sciebo-research/PLATOON/ENGIE-UBO-accident-use-case/TeamsData/TTL_files_of_CSV/Traffic_Accident_Injury_Database_2018/caracteristiques_2018_out_1.ttl"
    // val inputFilePath = "/Users/carstendraschner/Cloud/sciebo-research/PLATOON/ENGIE-UBO-accident-use-case/TeamsData/TTL_files_of_CSV/Traffic_Accident_Injury_Database_2018/*.ttl"
    val dataset = spark.rdf(Lang.TURTLE)(inputFilePath).toDS().cache()
    println(f"READ IN DATA:\ndata consists of ${dataset.count()} triples")
    dataset.take(n = 10).foreach(println(_))
    /*
    CREATE FEATURE EXTRACTING SPARQL
    from a knowledge graph we can either manually create a sparql query or
    we use the auto rdf2feature
     */
    // OPTION 1
    val manualSparqlString =
    """
      |SELECT
      |?accidentId
      |?accidentId__down_hasEnvironmentDescription__down_weatherCondition
      |?accidentId__down_hasEnvironmentDescription__down_colisionCondition
      |?accidentId__down_hasEnvironmentDescription__down_lightingCondition
      |?accidentId__down_location__down_subZoneOf__down_rdfschema_label
      |?accidentId__down_location__down_subZoneOf__down_rdfschema_label
      |?accidentId__down_location__down_schemaorgaddress__down_schemaorgpostalCode
      |?accidentId__down_location__down_schemaorgaddress__down_schemaorgstreetAddress
      |?accidentId__down_location__down_schemaorgaddress__down_schemaorgaddressCountry
      |?accidentId__down_location__down_wgs84_pos_point__down_wgs84_pos_lat
      |?accidentId__down_location__down_wgs84_pos_point__down_wgs84_pos_long
      |
      |WHERE {
      |	?accidentId <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.engie.fr/ontologies/accidentontology/RoadAccident> .
      |
      |	OPTIONAL {
      |		?accidentId <http://www.engie.fr/ontologies/accidentontology/hasEnvironmentDescription> ?accidentId__down_hasEnvironmentDescription .
      |		?accidentId__down_hasEnvironmentDescription <http://www.engie.fr/ontologies/accidentontology/weatherCondition> ?accidentId__down_hasEnvironmentDescription__down_weatherCondition .
      |	}
      |	OPTIONAL {
      |		?accidentId <http://www.engie.fr/ontologies/accidentontology/hasEnvironmentDescription> ?accidentId__down_hasEnvironmentDescription .
      |		?accidentId__down_hasEnvironmentDescription <http://www.engie.fr/ontologies/accidentontology/colisionCondition> ?accidentId__down_hasEnvironmentDescription__down_colisionCondition .
      |	}
      |	OPTIONAL {
      |		?accidentId <http://www.engie.fr/ontologies/accidentontology/hasEnvironmentDescription> ?accidentId__down_hasEnvironmentDescription .
      |		?accidentId__down_hasEnvironmentDescription <http://www.engie.fr/ontologies/accidentontology/lightingCondition> ?accidentId__down_hasEnvironmentDescription__down_lightingCondition .
      |	}
      |	OPTIONAL {
      |		?accidentId <https://w3id.org/seas/location> ?accidentId__down_location .
      |		?accidentId__down_location <https://w3id.org/seas/subZoneOf> ?accidentId__down_location__down_subZoneOf .
      |		?accidentId__down_location__down_subZoneOf <http://www.w3.org/2000/01/rdf-schema#label> ?accidentId__down_location__down_subZoneOf__down_rdfschema_label .
      |	}
      |	OPTIONAL {
      |		?accidentId <https://w3id.org/seas/location> ?accidentId__down_location .
      |		?accidentId__down_location <https://schema.orgaddress> ?accidentId__down_location__down_schemaorgaddress .
      |		?accidentId__down_location__down_schemaorgaddress <https://schema.orgpostalCode> ?accidentId__down_location__down_schemaorgaddress__down_schemaorgpostalCode .
      |	}
      |	OPTIONAL {
      |		?accidentId <https://w3id.org/seas/location> ?accidentId__down_location .
      |		?accidentId__down_location <https://schema.orgaddress> ?accidentId__down_location__down_schemaorgaddress .
      |		?accidentId__down_location__down_schemaorgaddress <https://schema.orgstreetAddress> ?accidentId__down_location__down_schemaorgaddress__down_schemaorgstreetAddress .
      |	}
      |	OPTIONAL {
      |		?accidentId <https://w3id.org/seas/location> ?accidentId__down_location .
      |		?accidentId__down_location <https://schema.orgaddress> ?accidentId__down_location__down_schemaorgaddress .
      |		?accidentId__down_location__down_schemaorgaddress <https://schema.orgaddressCountry> ?accidentId__down_location__down_schemaorgaddress__down_schemaorgaddressCountry .
      |	}
      |	OPTIONAL {
      |		?accidentId <https://w3id.org/seas/location> ?accidentId__down_location .
      |		?accidentId__down_location <http://www.w3.org/2003/01/geo/wgs84_pos#point> ?accidentId__down_location__down_wgs84_pos_point .
      |		?accidentId__down_location__down_wgs84_pos_point <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?accidentId__down_location__down_wgs84_pos_point__down_wgs84_pos_lat .
      |	}
      |	OPTIONAL {
      |		?accidentId <https://w3id.org/seas/location> ?accidentId__down_location .
      |		?accidentId__down_location <http://www.w3.org/2003/01/geo/wgs84_pos#point> ?accidentId__down_location__down_wgs84_pos_point .
      |		?accidentId__down_location__down_wgs84_pos_point <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?accidentId__down_location__down_wgs84_pos_point__down_wgs84_pos_long .
      |	}
      |}
      """.stripMargin
    // OPTION 2
    val (autoSparqlString: String, var_names: List[String]) = FeatureExtractingSparqlGenerator.createSparql(
      dataset,
      "?accidentId",
      "?accidentId <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.engie.fr/ontologies/accidentontology/RoadAccident> . ",
      1,
      3,
      10,
      featuresInOptionalBlocks = true)

    // select the query you want to use or adjust the automatic created one
    println("CREATE FEATURE EXTRACTING SPARQL")
    val queryString = manualSparqlString // autoSparqlString // manualSparqlString
    println()
    println(queryString)

    /*
    FEATURE EXTRACTION OVER SPARQL
    Gain Features from Query
    this creates a dataframe with coulms corresponding to Sparql features
     */
    println("FEATURE EXTRACTION OVER SPARQL")
    val sparqlFrame = new SparqlFrame()
      .setSparqlQuery(queryString)
      .setQueryExcecutionEngine(SPARQLEngine.Sparqlify)
    val res = sparqlFrame.transform(dataset)
    res.show(false)

    /*
    Create Numeric Feature Vectors
    */
    println("SMART VECTOR ASSEMBLER")
    val smartVectorAssembler = new SmartVectorAssembler()
      .setEntityColumn("accidentId")
      .setLabelColumn("accidentId__down_hasEnvironmentDescription__down_lightingCondition")
    val assembledDf = smartVectorAssembler.transform(res).cache()
    assembledDf.show(false)
    println(f"assembled df has ${assembledDf.count()} rows")

    /*
    APPLY Common SPARK MLlib Example Algorithm
     */
    println("APPLY Common SPARK MLlib Example Algorithm")
    // Trains a k-means model.
    val kmeans = new KMeans().setK(2) // .setSeed(1L)
    val model = kmeans.fit(assembledDf.distinct())

    // Make predictions
    val predictions = model.transform(assembledDf)
    predictions.show(false)

    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
  }
}
