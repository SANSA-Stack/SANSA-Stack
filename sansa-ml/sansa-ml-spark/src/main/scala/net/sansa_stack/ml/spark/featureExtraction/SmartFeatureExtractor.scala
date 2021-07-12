package net.sansa_stack.ml.spark.featureExtraction

import net.sansa_stack.ml.spark.featureExtraction.{FeatureExtractingSparqlGenerator, SmartVectorAssembler, SparqlFrame}
import net.sansa_stack.ml.spark.similarity.similarityEstimationModels.MinHashModel
import net.sansa_stack.ml.spark.utils.{FeatureExtractorModel, ML2Graph}
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.io.{NTripleReader, RDFReader}
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.graph
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}

object SmartFeatureExtractor {
  def main(args: Array[String]): Unit = {

    val input = args(0)

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

    import spark.implicits._

    var originalDataRDD: RDD[graph.Triple] = null
    if (input.endsWith("nt")) {
      originalDataRDD = NTripleReader
        .load(
          spark,
          input,
          stopOnBadTerm = ErrorParseMode.SKIP,
          stopOnWarnings = WarningParseMode.IGNORE
        )
    } else {
      val lang = Lang.TURTLE
      originalDataRDD = spark.rdf(lang)(input).persist()
    }

    val pivotFeatureDF = originalDataRDD
      .toDF()
      .groupBy("s")
      .pivot("p")
      .agg(collect_list("o"))
      .limit(1000)
    pivotFeatureDF
      .show(false)

    val newColNames: Array[String] = pivotFeatureDF.columns.map(_.replace(".", "_"))
    val df = pivotFeatureDF.toDF(newColNames: _*)

    val entityColumnNameString = "s"

    val featureColumns = df.columns.diff(Seq(entityColumnNameString))

    var joinDf = df.select(entityColumnNameString)

    for (featureColumn <- featureColumns) {
      println(featureColumn)
      val tmpDf = df.select(entityColumnNameString, featureColumn)
      val exDf = tmpDf.select(col(entityColumnNameString), explode(col(featureColumn)).as(featureColumn)) // TODO distinct to have mre unique subset to eval value type distribution

      exDf.show(false)
      exDf.printSchema()

      val sampleOfVals = exDf
        .select(featureColumn)
        .limit(10) // TODO nicer sampling strategy
      val possibleTypes: Dataset[String] = sampleOfVals.map(
        s => {
          if (s.toString().contains("^^")) "Literal" // TODO maybe check already here for literal type
          else if (s.toString().contains("http")) "URL"
          else "String"
        }
      )

      possibleTypes.foreach(println(_))

      val typeDistribution: Map[String, Int] = possibleTypes.collect().groupBy(identity).mapValues(_.size)
      typeDistribution.foreach(println(_))

      val castTypeAsString = if (typeDistribution.size == 1) typeDistribution.keys.head else "String"

      // exDf.withColumn("remLit", first(split(col(featureColumn), "^^"))).show(false)
      val someDf = exDf
        .withColumn("value", split(col(featureColumn), "\\^\\^")(0))
        .withColumn("litTypeUri", split(col(featureColumn), "\\^\\^")(1))
        .withColumn("litType", split(col("litTypeUri"), "\\#")(1))
        .na.fill(value = "string", Seq("litType"))
        // .withColumn("solo", first(col("remLit")))
      someDf
        .show(false)

      val litType: String = someDf.groupBy("litType").count().orderBy("count").rdd.map(r => r(0).toString).collect()(0)

      println("casting")
      println(litType)

      val castType = litType.toLowerCase() match {
        case "string" => "string"
        case "integer" => "integer"
        case "boolean" => "boolean"
        case "timestamp" => "timestamp"
        case _ => "string"
      }

      println(castType)

      val castedDf = someDf
        .withColumn("casted_value", col("value").cast(castType))
        .select(entityColumnNameString, "casted_value")

      castedDf.show(false)
      castedDf.printSchema()

      val newPivotedDf: DataFrame = someDf
        .groupBy(entityColumnNameString)
        .pivot("litType")
        .agg(collect_list("value"))
      newPivotedDf.show(false)

      // rename columns
      val currentColumnNames = newPivotedDf.columns
      val renamedCols = Seq(currentColumnNames.toSeq(0)) ++ currentColumnNames.drop(1).map(c => featureColumn.split("/").last + "_" + c).toSeq

      val renamedDf = newPivotedDf
        .toDF(renamedCols: _*)
      renamedDf
        .show(false)

      joinDf.show()

      joinDf = joinDf
        .join(renamedDf, Seq(entityColumnNameString), "left")
    }
    joinDf
      .show(false)
    joinDf
      .printSchema()
  }
}
