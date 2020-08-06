package net.sansa_stack.ml.spark.similarity.experiment

import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.apache.spark.sql.{DataFrame, SparkSession}

object MinimalServerExperiment {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(s"SimilarityPipelineExperiment")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val hdfsPath = args(0)
    val inputPath = hdfsPath

    val triples_df: DataFrame = spark.read.rdf(Lang.NTRIPLES)(inputPath)
    val inputFileSizeNumberTriples: Long = triples_df.count()
    println("\tthe file has " + inputFileSizeNumberTriples + " triples")
    triples_df.show()

    println("now we write the dataframe")
    val outputPath = inputPath + ".csv"
    triples_df.repartition(1).write.option("header", "true").format("csv").save(outputPath)
    spark.stop()
  }
}
