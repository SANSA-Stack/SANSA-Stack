package net.sansa_stack.ml.spark.similarity.run

import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.spark.SparkContext


object Semantic_Similarity_Estimator {

  // function which is called when object or this file is started
  // is implemented that it only manages a good input parameter handling
  // it uses the parser object to read in parameter and display help advices if needed
  // if parameter config is given, the function run is called which performs main purposes
  def main(args: Array[String]): Unit = {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String): Unit = {

    // set up spark
    val spark = SparkSession.builder
      .appName(s"Semantic Similarity Estimation example  $input") // TODO where is this displayed?
      .master("local[*]") // TODO why do we need to specify this?
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // TODO what is this for?
      .getOrCreate()

    // read in file with function and print certain inforamtion by function
    // code taken from triple reader
    val triples = read_in_nt_triples(
      input = input,
      spark = spark,
      lang = Lang.NTRIPLES
    )

    val exampleUris = List(
      "http://commons.dbpedia.org/resource/Template:Cc-by-1.0",
      "http://commons.dbpedia.org/resource/Category:Events",
      "http://commons.dbpedia.org/resource/Template:Cc-by-sa-1.0"
    )

    val resultMetaGraph: RDD[Triple] = semantic_similarity_estimation(
      spark = spark,
      uriA = exampleUris(0),
      uriB = exampleUris(1),
      triple_RDD = triples,
      mode = "random"
    )

    resultMetaGraph.foreach(println(_))

    spark.stop
    println("Spark session is stopped!")

  }

  /*def getAncestors(triples: RDD[Triple], uri: String): List[String] = {
    triples.filter(_.getObject() == uri).getSubjects().distinct
  }

  def getDescentants(triples: RDD[Triple], uri: String): List[String] = {
    triples.filter(_.getSubject() == uri).getObjects().distinct
  }

  def getNeighbors(triples: RDD[Triple], uri: String): List[String] = {
    val anc = getAncestors(triples, uri)
    val des = getDescentants(triples, uri)
    anc.union(des)
  }

  def getJaccardSimilarity(triples: RDD[Triple], uri1: String, uri2: String): Double = {
    intersection(getNeighbors(triples, uri1), getNeighbors(triples, uri2)) / union(getNeighbors(triples, uri1), getNeighbors(triples, uri2))
  }*/

  def getRandomSimilarity(triples: RDD[Triple], uri1: String, uri2: String): Double = {
    val r = scala.util.Random
    r.nextDouble()
  }

  def createMetaSimilarityResultGraph(spark: SparkSession, uri1: String, uri2: String, mode: String, similarity_value: Double): RDD[Triple] = {
    // TODO agree on good fitting uris for all predicates and objects

    val dt: String = "01.01.2020 - 14:15:33,232132" // TODO get real date time
    val experiment_type = "SimilarityEsimation"
    val similarityNodeName: String = uri1 + "_" + uri2 + "_" + mode + dt // TODO think of fitting hash

    val tmp_metaGraph: Array[Triple] = Array(
      Triple.create(
        NodeFactory.createURI(uri1),
        NodeFactory.createURI("used_in_experiment"),
        NodeFactory.createURI(similarityNodeName)
      ),Triple.create(
        NodeFactory.createURI(uri2),
        NodeFactory.createURI("used_in_experiment"),
        NodeFactory.createURI(similarityNodeName)
      ),Triple.create(
        NodeFactory.createURI(similarityNodeName),
        NodeFactory.createURI("performed"),
        NodeFactory.createURI(dt)
      ),Triple.create(
        NodeFactory.createURI(similarityNodeName),
        NodeFactory.createURI("experiment_type"),
        NodeFactory.createURI(experiment_type)
      ),Triple.create(
        NodeFactory.createURI(similarityNodeName),
        NodeFactory.createURI("mode"),
        NodeFactory.createURI(mode)
      )
      ,Triple.create(
        NodeFactory.createURI(similarityNodeName),
        NodeFactory.createURI("similarity"),
        NodeFactory.createLiteral(similarity_value.toString())
      )
    )
    val metaGraph: RDD[Triple] = spark.sparkContext.parallelize(tmp_metaGraph)
    metaGraph
  }

  def semantic_similarity_estimation(uriA: String, uriB: String, triple_RDD: RDD[graph.Triple], spark: SparkSession, mode: String = ""): RDD[Triple] = {
    println("calculate " + mode + " similarity of" + uriA + " and " + uriB)

    if (mode == "random") {
      val sv = getRandomSimilarity(triple_RDD, uriA, uriB)
      createMetaSimilarityResultGraph(spark = spark, uri1 = uriA, uri2 = uriB, mode = mode, similarity_value = sv)
    }
    /* else if (mode == "jaccard") {
      val sv = getJaccardSimilarity(triple_RDD, uriA, uriB)
      createMetaSimilarityResultGraph(spark = spark, uri1 = uriA, uri2 = uriB, mode = mode, similarity_value = sv)
    } */
    else {
      println("No fitting mode was set up: currently available are random and jaccard")
      spark.sparkContext.parallelize(List())
    }
  }

  //def uri_in_triples(uri: String, triples: RDD[graph.Triple]): Boolean = {}

  def read_in_nt_triples(input: String, spark: SparkSession, lang: Lang): RDD[graph.Triple] = {
    println("Read in file from " + input)

    // specify read in filetype, in this case: and nt file
    val lang = Lang.NTRIPLES
    println("The accepted fileformat seems to be hardcoded to " + lang)

    // read in triples with specified fileformat from input string path
    val triples = spark.rdf(lang)(input)
    println("file has been read in successfully!")

    println("5 Example Lines look like this:")

    triples.take(5).foreach(println(_))

    triples
  }

  //TODO what are the purposes for the following lines?
  case class Config(in: String = "")

  val parser = new scopt.OptionParser[Config]("Semantic Similarity Estimation example") {

    head(" Semantic Similarity Estimation example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")

    help("help").text("prints this usage text")
  }
}
