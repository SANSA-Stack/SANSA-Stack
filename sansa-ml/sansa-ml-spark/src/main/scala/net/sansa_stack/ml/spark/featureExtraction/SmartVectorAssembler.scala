package net.sansa_stack.ml.spark.featureExtraction

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.{StopWordsRemover, StringIndexer, Tokenizer, VectorAssembler, Word2Vec}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{Decimal, DoubleType, StringType, StructType}
import org.apache.spark.sql.functions.{udf, _}

import scala.collection.mutable

/**
 * This Transformer creates a needed Dataframe for common ML approaches in Spark MLlib.
 * The resulting Dataframe consists of a column features which is a numeric vector for each entity
 * The other columns are a identifier column like the node id
 * And optional column for label
 */
class SmartVectorAssembler extends Transformer{

  // column specifications

  // column name where entity is
  protected var _entityColumn: String = null
  // optional column name if some column should be seperately interpreted as label
  protected var _labelColumn: String = null
  // list of columns which should be used as features
  protected var _featureColumns: List[String] = null

  // working process onfiguration
  protected var _numericCollapsingStrategy: String = "median"
  protected var _stringCollapsingStrategy: String = "concat"

  // null replacement
  protected var _nullDigitReplacement: Int = -1
  protected var _nullStringReplacement: String = "nullString"

  protected var _word2VecSize = 2
  protected var _word2VecMinCount = 1

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

  /**
   * Set replacemnet for string or digit
   * @param datatype
   * @param value
   * @return
   */
  def setNullReplacement(datatype: String, value: Any): this.type = {
    if (datatype.toLowerCase() == "string") {
      _nullStringReplacement = value.toString
    }
    else if (datatype.toLowerCase == "digit") _nullDigitReplacement = {
      value.asInstanceOf[Int]
    }
    else {
      println("only digit and string are supported")
    }
    this
  }

  /**
   * setter for feature non categorical strings which are replaced by a word to vec
   * @param word2vecSize size of vector
   * @return transformer
   */
  def setWord2VecSize(word2vecSize: Int): this.type = {
    _word2VecSize = word2vecSize
    this
  }

  /**
   * setter for feature non categorical strings which are replaced by a word to vec
   * @param word2VecMinCount min number of min word occurencs
   * @return transformer
   */
  def setWord2VecMinCount(word2VecMinCount: Int): this.type = {
    _word2VecMinCount = word2VecMinCount
    this
  }

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

    val collapsedDataframe = dataset

    val availableColumns: Seq[String] = collapsedDataframe.columns.toSeq

    // first set entity column if it is not specified
    validateEntityColumn(cols = availableColumns)
    validateLabelColumn(cols = availableColumns)
    validateFeatureColumns(cols = availableColumns)

    var collectedFeatureColumns: Seq[String] = collapsedDataframe.columns.filterNot(_ == _entityColumn).toSeq
    if (_labelColumn != null) {
      collectedFeatureColumns = collectedFeatureColumns.filterNot(_ == _labelColumn)
    }

    var fullDigitizedDf: DataFrame = if (_labelColumn != null) {
      collapsedDataframe
        .select(_entityColumn, _labelColumn)
        .withColumnRenamed(_labelColumn, "label")
        .persist()
    }
    else {
      collapsedDataframe
      .select(_entityColumn)
      .persist()
    }


    collapsedDataframe.unpersist()


    for (featureColumn <- collectedFeatureColumns) {

      println(featureColumn)
      val featureType = featureColumn
        .split("\\(")(1)
        .split("\\)")(0)
      val featureName = featureColumn
        .split("\\(")(0)
      // println(featureName)
      // println(featureType)

      val dfCollapsedTwoColumns = collapsedDataframe
        .select(_entityColumn, featureColumn)

      var newFeatureColumnName: String = featureName
      val digitizedDf: DataFrame = if (featureType == "Single_NonCategorical_String") {
        newFeatureColumnName += "(Word2Vec)"

        val dfCollapsedTwoColumnsNullsReplaced = dfCollapsedTwoColumns
          .na.fill(_nullStringReplacement)

        val tokenizer = new Tokenizer()
          .setInputCol(featureColumn)
          .setOutputCol("words")

        val tokenizedDf = tokenizer
          .transform(dfCollapsedTwoColumnsNullsReplaced)
          .select(_entityColumn, "words")

        val remover = new StopWordsRemover()
          .setInputCol("words")
          .setOutputCol("filtered")

        val inputDf = remover
          .transform(tokenizedDf)
          .select(_entityColumn, "filtered")
          .cache()

        println(inputDf.count())
        inputDf.show(false)

        val word2vec = new Word2Vec()
          .setInputCol("filtered")
          .setOutputCol("output")
          .setMinCount(_word2VecMinCount)
          .setVectorSize(_word2VecSize)
        word2vec
          .fit(inputDf)
          .transform(inputDf)
          .withColumnRenamed("output", newFeatureColumnName)
          .select(_entityColumn, newFeatureColumnName)
      }
      else if (featureType == "ListOf_NonCategorical_String") {
        newFeatureColumnName += "(Word2Vec)"

        val dfCollapsedTwoColumnsNullsReplaced = dfCollapsedTwoColumns
          .na.fill(_nullStringReplacement)
          .withColumn("sentences", concat_ws(". ", col(featureColumn)))
          .select(_entityColumn, "sentences")

        val tokenizer = new Tokenizer()
          .setInputCol("sentences")
          .setOutputCol("words")

        val tokenizedDf = tokenizer
          .transform(dfCollapsedTwoColumnsNullsReplaced)

        val remover = new StopWordsRemover()
          .setInputCol("words")
          .setOutputCol("filtered")

        val inputDf = remover
          .transform(tokenizedDf)

        val word2vec = new Word2Vec()
          .setInputCol("filtered")
          .setOutputCol("output")
          .setMinCount(_word2VecMinCount)
          .setVectorSize(_word2VecSize)
        word2vec
          .fit(inputDf)
          .transform(inputDf)
          .withColumnRenamed("output", newFeatureColumnName)
          .select(_entityColumn, newFeatureColumnName)
      }
      else if (featureType == "Single_Categorical_String") {
        newFeatureColumnName += "(IndexedString)"

        val inputDf = dfCollapsedTwoColumns
          .na.fill(_nullStringReplacement)

        val indexer = new StringIndexer()
          .setInputCol(featureColumn)
          .setOutputCol("output")

        indexer
          .fit(inputDf)
          .transform(inputDf)
          .withColumnRenamed("output", newFeatureColumnName)
          .select(_entityColumn, newFeatureColumnName)
      }
      else if (featureType == "ListOf_Categorical_String") {
        newFeatureColumnName += "(ListOfIndexedString)"

        val inputDf = dfCollapsedTwoColumns
          .select(col(_entityColumn), explode_outer(col(featureColumn)))
          .na.fill(_nullStringReplacement)

        val indexer = new StringIndexer()
          .setInputCol("col")
          .setOutputCol("outputTmp")

        indexer
          .fit(inputDf)
          .transform(inputDf)
          .groupBy(_entityColumn)
          .agg(collect_list("outputTmp") as "output")
          .select(_entityColumn, "output")
          .withColumnRenamed("output", newFeatureColumnName)
          .select(_entityColumn, newFeatureColumnName)
      }
      else if (
        featureType.startsWith("ListOf") &&
          (featureType.endsWith("Double") || featureType.endsWith("Decimal") || featureType.endsWith("Int")  || featureType.endsWith("Integer"))
      ) {
        newFeatureColumnName += s"(${featureType})"

        dfCollapsedTwoColumns
          .select(col(_entityColumn), explode_outer(col(featureColumn)))
          .withColumnRenamed("col", "output")
          .na.fill(_nullDigitReplacement)
          .groupBy(_entityColumn)
          .agg(collect_list("output") as "output")
          .select(_entityColumn, "output")
          .withColumnRenamed("output", newFeatureColumnName)
          .select(_entityColumn, newFeatureColumnName)


      }
      else if (featureType.endsWith("Double")) {
        newFeatureColumnName += s"(${featureType})"

        dfCollapsedTwoColumns
          .withColumnRenamed(featureColumn, "output")
          .na.fill(_nullDigitReplacement)
          .select(_entityColumn, "output")
          .withColumnRenamed("output", newFeatureColumnName)
          .select(_entityColumn, newFeatureColumnName)
      }
      else if (featureType.endsWith("Integer")) {
        newFeatureColumnName += s"(${featureType})"

        dfCollapsedTwoColumns
          .withColumn("output", col(featureColumn).cast(DoubleType))
          // .withColumnRenamed(featureColumn, "output")
          .na.fill(_nullDigitReplacement)
          .select(_entityColumn, "output")
          .withColumnRenamed("output", newFeatureColumnName)
          .select(_entityColumn, newFeatureColumnName)
      }
      else if (featureType.endsWith("Boolean")) {
        newFeatureColumnName += s"(${featureType})"

        dfCollapsedTwoColumns
          .withColumn("output", col(featureColumn).cast(DoubleType))
          // .withColumnRenamed(featureColumn, "output")
          .na.fill(_nullDigitReplacement)
          .select(_entityColumn, "output")
          .withColumnRenamed("output", newFeatureColumnName)
          .select(_entityColumn, newFeatureColumnName)
      }
      else if (featureType.endsWith("Decimal")) {
        newFeatureColumnName += s"(${featureType})"

        dfCollapsedTwoColumns
          // .withColumn("output", col(featureColumn).cast(DoubleType))
          .withColumnRenamed(featureColumn, "output")
          .na.fill(_nullDigitReplacement)
          .select(_entityColumn, "output")
          .withColumnRenamed("output", newFeatureColumnName)
          .select(_entityColumn, newFeatureColumnName)
      }
      else {
        newFeatureColumnName += ("(notDigitizedYet)")

        println("transformation not possible yet")
        dfCollapsedTwoColumns
          .withColumnRenamed(featureColumn, "output")
          .withColumnRenamed("output", newFeatureColumnName)
          .select(_entityColumn, newFeatureColumnName)
      }

      /* val joinableDf: DataFrame = digitizedDf
        .withColumnRenamed("output", newFeatureColumnName)
        .select(_entityColumn, newFeatureColumnName)
       */

      fullDigitizedDf = fullDigitizedDf.join(
        digitizedDf,
        _entityColumn
      )
    }

    val allColumns: Array[String] = fullDigitizedDf.columns
    val nonDigitizedCoulumns: Array[String] = allColumns
      .filter(_.contains("(notDigitizedYet)"))
    val digitzedColumns: Array[String] = (allColumns diff nonDigitizedCoulumns)

    if (nonDigitizedCoulumns.size > 0) println(s"we drop following non digitized columns:\n${nonDigitizedCoulumns.mkString("\n")}")
    val onlyDigitizedDf = fullDigitizedDf
      .select(digitzedColumns.map(col(_)): _*)

    fullDigitizedDf.unpersist()

    // println("FIX FEATURE LENGTH")

    val columnsNameWithVariableFeatureColumnLength: Array[String] = onlyDigitizedDf.columns.filter(_.contains("ListOf"))

    var fixedLengthFeatureDf: DataFrame = onlyDigitizedDf
      .select((onlyDigitizedDf.columns diff columnsNameWithVariableFeatureColumnLength).map(col(_)): _*)
      .persist()

    // val fixedLengthFeatureDfSize = fixedLengthFeatureDf.count()

    for (columnName <- columnsNameWithVariableFeatureColumnLength) {
      // println(s"Fix number of features in column: $columnName")

      val newColumnName: String = columnName.split("\\(")(0)

      val twoColumnDf = onlyDigitizedDf.select(_entityColumn, columnName)

      val fixedLengthDf = twoColumnDf
        .select(col(_entityColumn), explode_outer(col(columnName)))
        .groupBy(_entityColumn)
        .agg(
          mean("col").alias(s"${newColumnName}_mean"),
          min("col").alias(s"${newColumnName}_min"),
          max("col").alias(s"${newColumnName}_max"),
          stddev("col").alias(s"${newColumnName}_stddev"),
        )
        .na.fill(_nullDigitReplacement) // this is needed cause stddev would result in Nan for empty list

      fixedLengthFeatureDf = fixedLengthFeatureDf.join(fixedLengthDf, _entityColumn)
    }

    // println("ASSEMBLE VECTOR")

    // TODO keep information about source for each vector entry s.t. it is explainable

    var columnsToAssemble: Array[String] = fixedLengthFeatureDf.columns.filterNot(_ == _entityColumn)
    if (_labelColumn != null) {
      columnsToAssemble = columnsToAssemble.filterNot(_ == "label")
    }
    // println(s"columns to assemble:\n${columnsToAssemble.mkString(", ")}")

    val assembler = new VectorAssembler()
      .setInputCols(columnsToAssemble)
      .setOutputCol("features")
    val assembledDf = assembler
      .transform(fixedLengthFeatureDf)
      .persist()

    fixedLengthFeatureDf.unpersist()

    val output = if (_labelColumn != null) {
      assembledDf
        .select(_entityColumn, "label", "features")
        .withColumnRenamed(_entityColumn, "entityID")
    }
    else {
      assembledDf
        .select(_entityColumn, "features")
        .withColumnRenamed(_entityColumn, "entityID")
    }

    output
  }
}
