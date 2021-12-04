package net.sansa_stack.examples.spark.ml.Similarity

import net.sansa_stack.ml.spark.featureExtraction.{SmartFeatureExtractor, SparqlFrame}
import net.sansa_stack.ml.spark.similarity.similarityEstimationModels.DaSimEstimator
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.sys.JenaSystem
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SimE4KG_DBpedia_Evaluation { // TODO this object is under development
  def main(args: Array[String]): Unit = {

    val path = args(0)

    // val modeF = args(1)
    // val modeE = args(2)

    val filter =
      """
        | SELECT ?seed WHERE {?seed <http://dbpedia.org/property/type> "Film" .}
        |""".stripMargin// args(3)
    // val extraction = args(4)

    // val objectFilter = "http://data.linkedmdb.org/movie/film"
    // val sparqlFilter = "SELECT ?seed WHERE {?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> .}"

    var currentTime: Long = System.nanoTime

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

    val timeSparkSetup = (System.nanoTime - currentTime) / 1e9d
    println(f"\ntime needed timeSparkSetup: ${timeSparkSetup}")
    currentTime = System.nanoTime

    // val originalDataRDD = spark.rdf(Lang.TURTLE)("/Users/carstendraschner/Datasets/lmdb.nt").persist()

    val dataset: Dataset[Triple] = NTripleReader
      .load(
        spark,
        path,
        stopOnBadTerm = ErrorParseMode.SKIP,
        stopOnWarnings = WarningParseMode.IGNORE
      )
      .toDS()
      .cache()

    println(dataset.count())

    dataset take 10 foreach println

    val timeRead = (System.nanoTime - currentTime) / 1e9d
    println(f"\ntime needed timeRead: ${timeRead}")
    currentTime = System.nanoTime

    val objectFilter = "\"Film\"@en"
    val predicateFilter = "http://dbpedia.org/property/type"

    // dataset.rdd.map(_.getObject.toString()).foreach(println(_))

    /* dataset
      // .filter (t => ((t.getPredicate.toString ().equals (predicateFilter) ) ) )
      .filter (t => ((t.getObject.toString ().equals (objectFilter) ) && (t.getPredicate.toString ().equals (predicateFilter) ) ) )
      .foreach(println(_))
     */

    val dse = new DaSimEstimator()
      // .setSparqlFilter("SELECT ?o WHERE { ?s <https://sansa.sample-stack.net/genre> ?o }")
      .setPredicateFilter("http://dbpedia.org/property/type")
      .setObjectFilter("\"Film\"@en")
      .setDistSimFeatureExtractionMethod("on")
      .setDistSimThreshold(0.5)
      .setSimilarityCalculationExecutionOrder(Array("starring", "prevYear", "runtime")) // Array("runtime")) //
      .setSimilarityValueStreching(false)
      // .setImportance(Map("initial_release_date_sim" -> 0.2, "rdf-schema#label_sim" -> 0.0, "runtime_sim" -> 0.2, "writer_sim" -> 0.1, "22-rdf-syntax-ns#type_sim" -> 0.0, "actor_sim" -> 0.3, "genre_sim" -> 0.2))
      // .setLimitSeeds(limitSeeds)

    val result: DataFrame = dse
      .transform(dataset)
      .cache()
    println("done with processing")

    result
      .select("uriA", "uriB", "overall_similarity_score")
      .show(false)

    /*
    val seeds: DataFrame = {
        println("filter by sparql")
        val sf = new SparqlFrame()
          .setSparqlQuery(filter)
        val tmpDf: DataFrame = sf
          .transform(dataset)
        val cn: Array[String] = tmpDf.columns
        tmpDf
          .withColumnRenamed(cn(0), "seed")
    }

    println("seeds count: ", seeds.count())

    val timeSe4kg = (System.nanoTime - currentTime) / 1e9d
    println(f"\ntime needed gather seeds: ${timeSe4kg}")
    currentTime = System.nanoTime

    val seedList: Array[String] = seeds.collect().map(_.getAs[String](0))

    seedList foreach println



    implicit val rowEncoder = Encoders.kryo(classOf[Row])
    implicit val nodeTupleEncoder = Encoders.kryo(classOf[(Node, Node, Node)])
    implicit val rdfTripleEncoder: Encoder[Triple] = org.apache.spark.sql.Encoders.kryo[Triple]

    val filteredDS: Dataset[Triple] = dataset
      .filter(r => seedList.contains(r.getSubject.toString())).map(_.asInstanceOf[Triple])
      .rdd
      .toDS()
      .cache()

    filteredDS.take(10).foreach(println(_))

    import net.sansa_stack.rdf.spark.model._

    filteredDS
      .rdd
      .toDF()
      .show()

    println("sample feature extraction SPARQL corresponding to SmartFeatureExtractor function")

    val tmpS1 = filteredDS
      .collect()
      .map(_.getPredicate.toString())
      .distinct
      .map(p => (p, "?" + p.split("/").last.replace("#", "_").replace(".", "").replace("-", "")))
      .sortWith(_._2 < _._2)
      // .take(7)

    val tmpPV = tmpS1
      .map(_._2)
    val tmpOB = tmpS1
      .map(t => ("OPTIONAL {?seed <" + t._1 + "> " + t._2.toString + " .}"))
    val tmpSP = "SELECT ?seed " + tmpPV.mkString(" ") + " \nWHERE {\n?seed ?p <" + filter + "> .\n" + tmpOB.mkString(" \n") + "}"
    println(tmpSP)

    println("now we do feature extraction")
    currentTime = System.nanoTime

    val featureDf = {
      if (modeE == "SparqlFrame") {
        println("DaSimEstimator: Feature Extraction by SparqlFrame")
        val sf = new SparqlFrame()
          .setSparqlQuery(tmpSP)
          .setCollapsByKey(true)
          .setCollapsColumnName("seed")
        val tmpDf = sf
          .transform(filteredDS)
        tmpDf
      }
      else {
        println("DaSimEstimator: Feature Extraction by SmartFeatureExtractor")

        implicit val rdfTripleEncoder: Encoder[Triple] = org.apache.spark.sql.Encoders.kryo[Triple]

        val sfe = new SmartFeatureExtractor()
          .setEntityColumnName("s")
        val feDf = sfe
          .transform(filteredDS)
        feDf
      }
    }
      .cache()

    featureDf.show()
    println("feature df count: ", featureDf.count())

    val timeFE = (System.nanoTime - currentTime) / 1e9d
    println(f"\ntime needed extract features: ${timeFE}")

*/

    spark.stop()

  }
}
