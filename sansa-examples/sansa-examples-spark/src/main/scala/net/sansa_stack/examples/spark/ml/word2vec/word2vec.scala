package net.sansa_stack.examples.spark.ml.word2vec

import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.{NTripleReader, RDFReader}
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.graph
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}


object Word2vec {
  def main(args: Array[String]): Unit = {

    // readIn
    val inputpath: String = args(0) // http://www.cs.toronto.edu/~oktie/linkedmdb/linkedmdb-18-05-2009-dump.nt

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

    // word2vec samples

    // val input = spark.sparkContext.textFile("data/mllib/sample_lda_data.txt").map(line => line.split(" ").toSeq)

    val model = Word2VecModel.load(spark.sparkContext, "/Users/carstendraschner/Downloads/GoogleNews-vectors-negative300.bin")

    val synonyms = model.findSynonyms("1", 5)

    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    println("\nREAD IN DATA")
    /**
     * Read in dataset of Jena Triple representing the Knowledge Graph
     */
    var originalDataRDD: RDD[graph.Triple] = null
    if (inputpath.endsWith("nt")) {
      originalDataRDD = NTripleReader
        .load(
          spark,
          inputpath,
          stopOnBadTerm = ErrorParseMode.SKIP,
          stopOnWarnings = WarningParseMode.IGNORE
        )
    } else {
      val lang = Lang.TURTLE
      originalDataRDD = spark.rdf(lang)(inputpath).persist()
    }
    val dataset: Dataset[graph.Triple] = originalDataRDD
      .toDS()
      .cache()

    println(f"\ndata consists of ${dataset.count()} triples")
    dataset
      .take(n = 10).foreach(println(_))
  }
}
