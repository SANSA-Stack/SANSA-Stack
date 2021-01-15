package net.sansa_stack.ml.spark.featureExtraction

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.functions.{udf, _}

/**
 * This Transformer creates a needed Dataframe for common ML approaches in Spark MLlib.
 * The resulting Dataframe consists of a column features which is a numeric vector for each entity
 * The other columns are a identifier column like the node id
 * And optional column for label
 */
class SmartVectorAssembler extends Transformer{

  // column specifications

  // column name where entity is
  var _entityColumn: String = null
  // optional column name if some column should be seperately interpreted as label
  var _labelColumn: String = null
  // list of columns which should be used as features
  var _featureColumns: List[String] = null

  // working process onfiguration
  var _numericCollapsingStrategy: String = "median"
  var _stringCollapsingStrategy: String = "concat"

  // null replacement
  var _nullReplacement: Int = -1

  protected val spark = SparkSession.builder().getOrCreate()

  // needed default elements
  override val uid: String = Identifiable.randomUID("sparqlFrame")
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
  override def transformSchema(schema: StructType): StructType =
    throw new NotImplementedError()

  // setters

  /**
   * set which columns represents the entity
   * if not set first column is used
   * @param p entity columnName as string
   * @return set transformer
   */
  def setEntityColumn(p: String): this.type = {
    _entityColumn = p
    this
  }

  /**
   * set which columns represents the labl, if not set no label column
   * @param p label columnName as string
   * @return set transformer
   */
  def setLabelColumn(p: String): this.type = {
    _labelColumn = p
    this
  }

  /**
   * set which columns represents the features, if not set all but label and entity are used
   * @param p label columnName as string
   * @return set transformer
   */
  def setFeatureColumns(p: List[String]): this.type = {
    _featureColumns = p
    this
  }

  /*
  TODO needed setters setDropNull
  setNumericCollapsingStrategy
  setStringCollapsingStrategy
   */

  /**
   * Validate set column to check if we need fallback to first column if not set
   * and if set if it is in available cols
   *
   * @param cols the available columns
   */
  def validateEntityColumn(cols: Seq[String]): Unit = {
    if (_entityColumn == null) {
      _entityColumn = cols(0)
      println("SmartVectorAssembler: No entity column has been set, that's why the first column is used as entity column")
    }
    else {
      if (!cols.contains(_entityColumn)) {
        throw new Exception(f"the set entityColumn: ${_entityColumn} is not in available columns ${cols.mkString(", ")}")
      }
    }
  }

  /**
   * validate if label is in available columns
   * @param cols the avaiable columns
   */
  def validateLabelColumn(cols: Seq[String]): Unit = {
    if (_labelColumn != null) {
      if (!cols.contains(_labelColumn)) {
        throw new Exception(f"the set entityColumn: ${_labelColumn} is not in available columns ${cols.mkString(", ")}")
      }
    }
  }

  /**
   * validate the feature columns
   * if feature columns are set, check if those are in avaiable columns
   * if not raise exception
   * if not set determine feature columns by all columns minus the label and entty column
   * @param cols
   */
  def validateFeatureColumns(cols: Seq[String]): Unit = {
    if (_featureColumns != null) {
      val nonAvailableFeatures = _featureColumns.toSet.diff(cols.toSet)
      if (nonAvailableFeatures.size > 0) {
        throw new Exception(f"the set featureColumns: ${_featureColumns} has $nonAvailableFeatures which are not in available columns $cols")
      }
    }
    else {
      val generatedColumnList = cols
        .filterNot(elm => elm == _labelColumn)
        .filterNot(elm => elm == _entityColumn)
      _featureColumns = generatedColumnList.toList
      println(s"SmartVectorAssembler: no feature Columns are set -> automatic retrieved columns: ${_featureColumns}")
    }
    if(_featureColumns.size == 0) {
      throw new Exception("The size feature columns is zero")
    }
  }

  /**
   * transforms a dataframe of query results to a numeric feature vectors and a id and label column
   * @param dataset dataframe with columns for id features and optional label
   * @return dataframe with columns id features and optional label where features are numeric vectors which incooperate with mllib
   */
  def transform(dataset: Dataset[_]): DataFrame = {

    val df = dataset.toDF()

    val allColumns = df.columns.toSeq

    // first set entity column if it is not specified
    validateEntityColumn(cols = allColumns)
    validateLabelColumn(cols = allColumns)
    validateFeatureColumns(cols = allColumns)

    // replace strings by numeric values
    var digitizedDf = df
    val dfFeaturesSchemaMap = digitizedDf.schema.map(e => (e.name, e.dataType)).toMap

    for (columnName <- digitizedDf.columns) {
      if ((columnName != _entityColumn) && (columnName != _labelColumn)) {

        // decide what to do
        if (dfFeaturesSchemaMap(columnName) ==  StringType) {
          val indexer = new StringIndexer()
            .setInputCol(columnName)
            .setOutputCol("string2indexed_" + columnName)
            .setHandleInvalid("keep") // for null values
            .fit(digitizedDf)
          digitizedDf = indexer
            .transform(digitizedDf)
          digitizedDf = digitizedDf.drop(columnName)

          val indexOfUnknown = indexer.labelsArray(0).size

          digitizedDf = digitizedDf.withColumn("string2indexed_" + columnName,
              when(col("string2indexed_" + columnName) === indexOfUnknown.toDouble, null)
                .otherwise(col("string2indexed_" + columnName)
                )
            )
        }
      }
    }

    // set tmpFeatureColumns
    var tmpFeatureColumns = digitizedDf.columns
      .filterNot(elm => elm == _labelColumn)
      .filterNot(elm => elm == _entityColumn)

    /*
    // collaps multiple rows for same entity
    println("collaps multiple rows for same entity")
    val groupCol = _entityColumn
    val aggCols = (tmpFeatureColumns.toSet - groupCol).map(
      colName => collect_list(colName).as("collect_list(" + colName + ")")
    ).toList
    var groupedDf = digitizedDf.groupBy(groupCol).agg(aggCols.head, aggCols.tail: _*)
    groupedDf.show(false)

    // set tmpColapedFeatureColumns
    tmpFeatureColumns = groupedDf.columns
      .filterNot(elm => elm == _labelColumn)
      .filterNot(elm => elm == _entityColumn)

    // decision what to do with lists
    // println(groupedDf.schema)
    val randomSampleFromArray = udf((a) => {
      println(a.getClass)
      // Random.shuffle(a.toList).take(1)
    })
    for (columnName <- tmpColapedFeatureColumns) {
      groupedDf = groupedDf.withColumn(columnName, randomSampleFromArray(col(columnName)))
    }
     */

    // Handle Null values
    digitizedDf = digitizedDf.na.fill(_nullReplacement)

    // assemble feature vector
    val assembler = new VectorAssembler()
      .setInputCols(tmpFeatureColumns)
      .setOutputCol("features").setHandleInvalid("keep")
    val assembledDf = assembler.transform(digitizedDf)

    var cleanDf = assembledDf
    if (_labelColumn == null) {
      cleanDf = cleanDf
        .withColumnRenamed(_entityColumn, "id")
        .select("id", "features")
    }
    else {
      cleanDf = cleanDf
        .withColumnRenamed(_entityColumn, "id")
        .withColumnRenamed(_labelColumn, "label")
        .select("id", "label", "features")
    }
    cleanDf
  }
}
