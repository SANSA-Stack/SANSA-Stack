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
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}

class SmartFeatureExtractor extends Transformer {
  val spark = SparkSession
    .builder
    .getOrCreate()

  var entityColumnNameString = "s"

  def setEntityColumnName(colName: String): this.type = {
    entityColumnNameString = colName
    this
  }

  def transform(dataset: Dataset[_]): DataFrame = {

    implicit val rdfTripleEncoder: Encoder[Triple] = org.apache.spark.sql.Encoders.kryo[Triple]

    /**
     * expand initial DF to its features by one hop
     */
    val pivotFeatureDF = dataset
      .rdd
      .map(_.asInstanceOf[Triple])
      .toDF() // make a DF out of dataset
      .groupBy("s")
      .pivot("p") // create columns for each predicate as kind of respective features
      .agg(collect_list("o")) // collect these features in list

    // need to rename cols so internal SQL does not have issues
    val newColNames: Array[String] = pivotFeatureDF
      .columns
      .map(_.replace(".", "_"))
    val df = pivotFeatureDF
      .toDF(newColNames: _*)

    /**
     * these are the feature columns we iterate over
     */
    val featureColumns = df.columns
      .diff(Seq(entityColumnNameString))

    /**
     * This is the dataframe where we join the casted columns
     */
    var joinDf = df.select(entityColumnNameString)

    // iterate over each feature column
    for (featureColumn <- featureColumns) {
      /**
       * two column df so ntity column with one additional column
       */
      var tmpDf: DataFrame = df
        .select(entityColumnNameString, featureColumn) // make two col df
        .select(col(entityColumnNameString), explode(col(featureColumn)).as(featureColumn)) // exlode to get access to all vals
        .withColumn("value", split(col(featureColumn), "\\^\\^")(0)) // gather the values
        .withColumn("litTypeUri", split(col(featureColumn), "\\^\\^")(1)) // get the litTypes by splitting the lit representation
        .withColumn("litType", split(col("litTypeUri"), "\\#")(1)) // the datatype especially is often annotated after the hashtag
        .na.fill(value = "string", Seq("litType")) // fallback to string, this does not only apply to non annotated literals but also to URIs or blanks
        .groupBy(entityColumnNameString) // group again
        .pivot("litType") // now expand by lit type s.t. we have for each featrutre maybe multiple cols if they are corresponding to different lit types
        .agg(collect_list("value")) // collect back again these features

      val currentFeatureCols: Array[String] = tmpDf
        .columns
        .drop(1)

      currentFeatureCols.foreach(f = cn => {
        val castType: String = cn.toLowerCase() match {
          case "string" => "string"
          case "integer" => "double"
          case "boolean" => "double"
          case "double" => "double"
          case "int" => "double"
          case "float" => "double"
          case "timestamp" => "timestamp"
          case _ => "string"
        }
        val newFC: String = if (currentFeatureCols.size == 1) featureColumn.split("/").last else featureColumn.split("/").last + "_" + castType
        tmpDf = tmpDf
          .withColumn(newFC, col(cn).cast("array<" + castType + ">")) // cast the respective cols to their identified feature cols
          .drop(cn) // drop the old col

        val maxNumberElements = tmpDf
          .select(col(entityColumnNameString), explode(col(featureColumn.split("/").last)))
          .select(entityColumnNameString)
          .groupBy(entityColumnNameString)
          .count()
          .agg(max("count"))
          .select("max(count)")
          .first()
          .getLong(0)

        if (maxNumberElements == 1) {
          tmpDf = tmpDf.select(col(entityColumnNameString), explode(col(tmpDf.columns.last)).as(tmpDf.columns.last))
        }
      })

      joinDf = joinDf
        .join(tmpDf, Seq(entityColumnNameString), "left")
    }
    joinDf
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  override val uid: String = "FIXME"
}
