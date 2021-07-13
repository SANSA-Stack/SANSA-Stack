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

  var entityColumnNameString = "s"

  def transform(dataset: Dataset[_]): DataFrame = {
    /**
     * expand initial DF to its features by one hop
     */
    val pivotFeatureDF = dataset
      .toDF()
      .groupBy("s")
      .pivot("p")
      .agg(collect_list("o"))

    // need to rename cols so internal SQL does not have issues
    val newColNames: Array[String] = pivotFeatureDF
      .columns
      .map(_.replace(".", "_"))
    val df = pivotFeatureDF
      .toDF(newColNames: _*)

    /**
     * these are the feature columns we iterate over
     */
    val featureColumns = df.columns.diff(Seq(entityColumnNameString))

    /**
     * This is the dataframe where we join the casted columns
     */
    var joinDf = df.select(entityColumnNameString)

    // iterate over each feature column
    for (featureColumn <- featureColumns) {
      /**
       * two column df so ntity column with one additional column
       */
      val tmpDf = df
        .select(entityColumnNameString, featureColumn)
      val exDf = tmpDf
        .select(col(entityColumnNameString), explode(col(featureColumn)).as(featureColumn)) // TODO distinct to have mre unique subset to eval value type distribution

      val someDf = exDf
        .withColumn("value", split(col(featureColumn), "\\^\\^")(0))
        .withColumn("litTypeUri", split(col(featureColumn), "\\^\\^")(1))
        .withColumn("litType", split(col("litTypeUri"), "\\#")(1))
        .na.fill(value = "string", Seq("litType"))

      var pvdf = someDf
        .groupBy(entityColumnNameString)
        .pivot("litType")
        .agg(collect_list("value"))

      val currentFeatureCols = pvdf.columns.drop(1)

      currentFeatureCols.foreach(cn => {
        val castType = cn.toLowerCase() match {
          case "string" => "string"
          case "integer" => "integer"
          case "boolean" => "boolean"
          case "timestamp" => "timestamp"
          case _ => "string"
        }
        val newFC = if (currentFeatureCols.size == 1) featureColumn.split("/").last else featureColumn.split("/").last + "_" + castType
        pvdf = pvdf
          .withColumn(newFC, col(cn).cast("array<" + castType + ">"))
          .drop(cn)
      })

      joinDf = joinDf
        .join(pvdf, Seq(entityColumnNameString), "left")
    }
    joinDf
  }

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

    val inDf = originalDataRDD
      .toDF()

    val outDf = transform(inDf)
    outDf
      .show()
    outDf
      .printSchema()
  }
}
