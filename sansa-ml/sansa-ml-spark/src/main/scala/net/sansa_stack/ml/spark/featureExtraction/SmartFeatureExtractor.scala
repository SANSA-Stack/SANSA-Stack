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
import org.apache.spark.sql.functions.{get => sparkGet, _}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}

class SmartFeatureExtractor extends Transformer {
  val spark = SparkSession
    .builder
    .getOrCreate()

  var entityColumnNameString = "s"

  // filter
  var objectFilter: String = ""
  var predicateFilter: String = ""
  var sparqlFilter: String = ""

  def setEntityColumnName(colName: String): this.type = {
    entityColumnNameString = colName
    this
  }

  def setObjectFilter(filter: String): this.type = {
    objectFilter = filter
    this
  }

  def setPredicateFilter(filter: String): this.type = {
    predicateFilter = filter
    this
  }

  def setSparqlFilter(filter: String): this.type = {
    sparqlFilter = filter
    this
  }

  def transform(dataset: Dataset[_]): DataFrame = {
    implicit val rdfTripleEncoder: Encoder[Triple] = org.apache.spark.sql.Encoders.kryo[Triple]

    val ds = dataset.asInstanceOf[Dataset[Triple]]

    val filteredDataset: Dataset[Triple] = {
      if (objectFilter != "" || predicateFilter != "") {

        val seeds: DataFrame = {
          val tmpF = {
            if (objectFilter != "" && predicateFilter != "") ds.filter(t => ((t.getObject.toString().equals(objectFilter)) && (t.getPredicate.toString().equals(predicateFilter))))
            else if (objectFilter != "" && predicateFilter == "") ds.filter(t => ((t.getObject.toString().equals(objectFilter))))
            else ds.filter(t => (t.getPredicate.toString().equals(predicateFilter)))
          }
          tmpF
            .rdd
            .toDF()
            .select("s")
            .withColumnRenamed("s", "seed")
        }

        /* val seeds: DataFrame = dataset
          .map(_.asInstanceOf[Triple])
          .filter(t => ((t.getObject.toString().equals(objectFilter))))
          .rdd
          .toDF()
          .select("s")
          .withColumnRenamed("s", "seed") */
        val seedList: Array[String] = seeds.collect().map(_.getAs[String](0))

        dataset
          .map(_.asInstanceOf[Triple])
          .filter(r => seedList.contains(r.getSubject.toString())).map(_.asInstanceOf[Triple])
          .rdd
          .map(_.asInstanceOf[Triple])
          .toDS()
      }
      else if (sparqlFilter != "") {
        val sf = new SparqlFrame()
          .setSparqlQuery(sparqlFilter)
        val seeds: DataFrame = sf
          .transform(dataset)
        val seedList: Array[String] = seeds.collect().map(_.getAs[String](0))

        dataset
          .map(_.asInstanceOf[Triple])
          .filter(r => seedList.contains(r.getSubject.toString()))
          .map(_.asInstanceOf[Triple])
          .rdd
          .map(_.asInstanceOf[Triple])
          .toDS()
      }
      else {
        dataset
          .rdd
          // .map(_.asInstanceOf[Triple])
          .asInstanceOf[RDD[org.apache.jena.graph.Triple]]
          .toDS()
      }
    }.cache()

    /**
     * expand initial DF to its features by one hop
     */
    val pivotFeatureDF = filteredDataset
      .rdd
      .asInstanceOf[RDD[org.apache.jena.graph.Triple]]
      .toDF() // make a DF out of dataset
      .groupBy("s")
      .pivot("p") // create columns for each predicate as kind of respective features
      .agg(collect_list("o")) // collect these features in list

    // pivotFeatureDF.show()

    // need to rename cols so internal SQL does not have issues
    val newColNames: Array[String] = pivotFeatureDF
      .columns
      .map(_.replace(".", "_"))
    val df = pivotFeatureDF
      .toDF(newColNames: _*)
      .cache()

    // df.show()

    /**
     * these are the feature columns we iterate over
     */
    val featureColumns = df.columns
      .diff(Seq(entityColumnNameString))

    /**
     * This is the dataframe where we join the casted columns
     */
    var joinDf = df
      .select(entityColumnNameString)
      .cache()

    // iterate over each feature column
    for (featureColumn <- featureColumns) {
      /**
       * two column df so ntity column with one additional column
       */
      var tmpDf: DataFrame = df
        .select(entityColumnNameString, featureColumn) // make two col df
        .select(col(entityColumnNameString), explode(col(featureColumn)).as(featureColumn)) // exlode to get access to all vals
        .withColumn("value", sparkGet(split(col(featureColumn), "\\^\\^"), lit(0))) // gather the values
        .withColumn("litTypeUri", sparkGet(split(col(featureColumn), "\\^\\^"), lit(1))) // get the litTypes by splitting the lit representation
        .withColumn("litType", sparkGet(split(col("litTypeUri"), "\\#"), lit(1))) // the datatype especially is often annotated after the hashtag
        .na.fill(value = "string", Seq("litType")) // fallback to string, this does not only apply to non annotated literals but also to URIs or blanks
        .groupBy(entityColumnNameString) // group again
        .pivot("litType") // now expand by lit type s.t. we have for each featrutre maybe multiple cols if they are corresponding to different lit types
        .agg(collect_list("value")) // collect back again these features

      // println("featureColumn: ", featureColumn)
      // tmpDf.show(false)

      val currentFeatureCols: Array[String] = tmpDf
        .columns
        .drop(1)

      // currentFeatureCols.foreach(println(_))

      // val castedDf = tmpDf.select(entityColumnNameString)

      currentFeatureCols.foreach(cn => {
        // println("cn", cn)
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
        val newFC: String = if (currentFeatureCols.size == 1) featureColumn.split("/").last else featureColumn.split("/").last + "_" + castType // if (currentFeatureCols.size == 1) Array(featureColumn + _ + cn) else currentFeatureCols.map(_ + "_" + castType)
        // println("newFC", newFC)
        tmpDf = tmpDf
          .withColumn(newFC, col(cn).try_cast("array<" + castType + ">")) // cast the respective cols to their identified feature cols
          .drop(cn) // drop the old col

        val maxNumberElements = tmpDf
          .select(col(entityColumnNameString), explode(col(newFC)))
          .select(entityColumnNameString)
          .groupBy(entityColumnNameString)
          .count()
          .agg(max("count"))
          .select("max(count)")
          .first()
          .getLong(0)

        if (maxNumberElements == 1) {
          tmpDf = tmpDf
            .withColumnRenamed(newFC, "oldCol")
            .withColumn(newFC, sparkGet(col("oldCol"), lit(0)))
            .drop("oldCol")
            // .select(col(entityColumnNameString), explode(col(tmpDf.columns.last)).as(tmpDf.columns.last))
        }
        // castedDf.join(tmpDf, Seq(entityColumnNameString), "left")
      })

      // println("casted and renamed sub df")
      // tmpDf
        // .show()

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
