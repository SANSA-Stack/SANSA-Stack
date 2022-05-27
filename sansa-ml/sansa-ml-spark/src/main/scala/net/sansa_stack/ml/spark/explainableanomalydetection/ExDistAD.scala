package net.sansa_stack.ml.spark.explainableanomalydetection

import net.sansa_stack.ml.spark.anomalydetection.DistADLogger.LOG
import org.apache.jena.graph
import org.apache.spark.ml.feature.{VectorAssembler}
import org.apache.spark.ml.regression.{
  DecisionTreeRegressionModel,
  DecisionTreeRegressor
}
import org.apache.spark.ml.tree._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

object ExDistAD {

  var fileCounter = 1;

  /**
    * Get a dataframe and array of columns and generated a vector for the given columns
    * @param featureColumns the given columns
    * @param data a given dataframe
    * @return a tuple of dataframe and new columns
    */
  def assembleDF(
      featureColumns: Array[String],
      data: DataFrame
  ): (DataFrame, Array[String]) = {
    val assembler = new VectorAssembler()
      .setInputCols(featureColumns)
      .setOutputCol("indexedFeatures")
      .setHandleInvalid("skip")

    val output = assembler.transform(data)
    output.createOrReplaceTempView("data")
    (output, assembler.getInputCols)
  }

  /**
    * Create a Decision Tree and tran it
    * @param data the given data
    * @param labelColumn the given label
    * @param config the config object
    * @return the trained DecisionTreeRegressionModel
    */
  def trainDecisionTree(
      data: DataFrame,
      labelColumn: String,
      config: ExDistADConfig
  ): DecisionTreeRegressionModel = {
    val output = data.filter(!col(labelColumn).isNull)
    val dtRegressor = new DecisionTreeRegressor()
      .setLabelCol(labelColumn)
      .setFeaturesCol("indexedFeatures")
      .setMaxBins(config.maxBin)
      .setMaxDepth(config.maxDepth)
      .setCacheNodeIds(true)

    LOG.info(s"DecisionTree training for $labelColumn ....")
    val treeModel = dtRegressor.fit(output)
    treeModel
  }

  /**
    * Gets a trained DecisionTreeRegressionModel and parse it and convert each node to a query
    * @param treeModel the trained DecisionTreeRegressionModel
    * @param output
    * @param realColsName
    * @return list of rules
    */
  def parseDecisionTree(
      treeModel: DecisionTreeRegressionModel,
      output: DataFrame,
      realColsName: Array[String]
  ): List[String] = {
    var rawRules: List[String] = List[String]()
    var rootNode: Node = treeModel
      .asInstanceOf[DecisionTreeRegressionModel]
      .rootNode
    if (rootNode.isInstanceOf[LeafNode]) {
      return rawRules
    } else {
      rootNode = rootNode.asInstanceOf[InternalNode]
    }

    var stack: List[(org.apache.spark.ml.tree.Node, String)] =
      List[(org.apache.spark.ml.tree.Node, String)]()
    stack = (rootNode, "") :: stack
    var i = 0
    while (stack.nonEmpty) {
      val head = stack.head
      val node = head._1
      stack = stack.tail
      i = i + 1
      var query: String = head._2
      if (node.isInstanceOf[LeafNode]) {
        rawRules = query.substring(1) :: rawRules
      } else {
        if (query.nonEmpty) {
          rawRules = query.substring(1) :: rawRules
        }
        val split: Split = node.asInstanceOf[InternalNode].split
        var appendQueryRight: String = ""
        var appendQueryLeft: String = ""
        if (split.isInstanceOf[ContinuousSplit]) {
          val threshold = split.asInstanceOf[ContinuousSplit].threshold
          val featureIndex =
            split.asInstanceOf[ContinuousSplit].featureIndex
          appendQueryRight = ";feature " + featureIndex + " > " + threshold
          appendQueryLeft = ";feature " + featureIndex + " <= " + threshold
        } else if (split.isInstanceOf[CategoricalSplit]) {
          val featureIndex =
            split.asInstanceOf[CategoricalSplit].featureIndex
          val rightCats =
            split.asInstanceOf[CategoricalSplit].rightCategories
          val leftCats = split.asInstanceOf[CategoricalSplit].leftCategories

          if (output
                .schema(realColsName(featureIndex))
                .dataType == BooleanType) {
            appendQueryRight = ";feature " + featureIndex + " == " + rightCats
              .mkString(",")
            appendQueryLeft = ";feature " + featureIndex + " == " + leftCats
              .mkString(",")
          } else {
            appendQueryRight = ";feature " + featureIndex + " is in [" + rightCats
              .mkString(",") + "]"
            appendQueryLeft = ";feature " + featureIndex + " is in [" + leftCats
              .mkString(",") + "]"
          }

        }
        stack =
          (
            node
              .asInstanceOf[InternalNode]
              .rightChild,
            query + appendQueryRight
          ) :: stack
        stack =
          (
            node
              .asInstanceOf[InternalNode]
              .leftChild,
            query + appendQueryLeft
          ) :: stack
      }

    }
    LOG.info("DecisionTree has been parsed for generating rules")
    rawRules
  }

  def getRealValue(
      featureName: String,
      featureValue: String,
      output: DataFrame
  ): Object = {
    if (featureName.endsWith("__boolean")) {
      featureValue
    } else {
      val newFeatureName = featureName.replace("_index", "")
      val result = output
        .select(featureName, newFeatureName)
        .where(col(featureName) === lit(featureValue))
        .first()
        .getString(1)
      result
//
//      val converter = new IndexToString()
//        .setInputCol(featureName)
//        .setOutputCol("originalValue")
//      val converted: DataFrame = converter.transform(output)
//      converted.show(false)
//      val selectedData = converted
//        .select(newFeatureName, featureName, "originalValue")
//
//      selectedData.show(false)
//
//      featureValue
    }
  }

  /**
    * Gets list of rules and convert them to SQL rules
    *
    * @param rawRules a given list of rules
    * @param realColsName
    * @return the list of SQL rules
    */
  def generateSqlRules(
      rawRules: List[String],
      realColsName: Array[String],
      output: DataFrame
  ): (List[String], List[String]) = {
    var sqlRules: List[String] = List[String]()
    var sqlMappedRules: List[String] = List[String]()
    for (rule <- rawRules) {
      var resultSqlRule: String = ""
      var resultSqlMappedRule: String = ""
      val splitArray: Array[String] = rule.split(";")
      for (r <- splitArray) {
        if (r.contains("is in")) {
          // categorical
          val rSplit = r.split(" ")
          val featureIndex = rSplit(1).toInt
          val featureValue = rSplit(4).substring(1, rSplit(4).length - 1)
          if (resultSqlRule.isEmpty) {
            resultSqlRule = resultSqlRule + realColsName(featureIndex) + " IN (" + featureValue + ")"
            resultSqlMappedRule = resultSqlMappedRule + realColsName(
              featureIndex
            ).replace("_index", "") + " IN (" + getRealValue(
              realColsName(featureIndex),
              featureValue,
              output
            ) + ")"
          } else {
            resultSqlRule = resultSqlRule + " AND " + realColsName(
              featureIndex
            ) + " IN (" + featureValue + ")"
            resultSqlMappedRule = resultSqlMappedRule + " AND " + realColsName(
              featureIndex
            ).replace("_index", "") + " IN (" + getRealValue(
              realColsName(featureIndex),
              featureValue,
              output
            ) + ")"
          }
        } else {
          // numerical
          val rSplit = r.split(" ")
          val featureIndex = rSplit(1).toInt
          val operator = rSplit(2)
          val featureValue = rSplit(3).toDouble
          if (resultSqlRule.isEmpty) {
            resultSqlRule = resultSqlRule + realColsName(featureIndex) + " " + operator + " " + featureValue
            resultSqlMappedRule = resultSqlMappedRule + realColsName(
              featureIndex
            ) + " " + operator + " " + featureValue
          } else {
            val checkString = realColsName(featureIndex) + " " + operator + " "
            if (resultSqlRule.contains(checkString)) {
              resultSqlRule = resultSqlRule.replaceAll(
                checkString + "\\d+\\.?\\d+",
                realColsName(
                  featureIndex
                ) + " " + operator + " " + featureValue
              )

              resultSqlMappedRule = resultSqlMappedRule.replaceAll(
                checkString + "\\d+\\.?\\d+",
                realColsName(
                  featureIndex
                ) + " " + operator + " " + featureValue
              )
            } else {
              resultSqlRule = resultSqlRule + " AND " + realColsName(
                featureIndex
              ) + " " + operator + " " + featureValue
              resultSqlMappedRule = resultSqlMappedRule + " AND " + realColsName(
                featureIndex
              ) + " " + operator + " " + featureValue
            }

          }
        }
      }
      sqlRules = resultSqlRule :: sqlRules
      sqlMappedRules = resultSqlMappedRule :: sqlMappedRules
    }
    (sqlRules, sqlMappedRules)
  }

  /**
    * Sort SQL rules based on length
    * @param sqlRules the given SQL rules
    * @return sorted SQL rules
    */
  def sortSqlRulesDesc(sqlRules: List[String]): List[String] = {
    sqlRules.sortWith(
      _.sliding("AND".length).count(_ == "AND") > _.sliding("AND".length)
        .count(_ == "AND")
    )

  }

  /**
    * Check to see if a column name contains "index"
    * @param columnName
    * @param allColumns
    * @return true or false
    */
  def containIndex(columnName: String, allColumns: Array[String]): Boolean = {
    for (col <- allColumns) {
      if (col.contains(columnName) && col.endsWith("_index") && !columnName
            .endsWith("_index")) {
        return true
      }
    }
    false
  }

  /**
    * The main function
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val config: ExDistADConfig = new ExDistADConfig(
      args(0)
    )
    val spark = ExDistADUtil.createSpark()
    LOG.info(config)
    val input = config.inputData
    if (config.verbose) {
      LOG.info("Input file is: " + input)
    }
    var originalDataRDD: RDD[graph.Triple] =
      ExDistADUtil.readData(spark, input).repartition(200).cache()
    if (config.verbose) {
      LOG.info("Original Data RDD:")
      originalDataRDD.take(10) foreach LOG.info
    }

    val data = ExSmartDataFrame.transform(originalDataRDD, config)
    if (config.verbose) {
      LOG.info("Transformed Data to SmartDataFrame:")
      data.show(false)
    }

    val allColumns: Array[String] = data.columns
    val numericalColumns =
      allColumns
        .filter(
          p =>
            p.endsWith("__int") || p.endsWith("__integer") || p.endsWith(
              "__decimal"
            )
        )

    // Safety check to ignore all the ids
    val numericalColumnsWithoutIdColumn: Array[String] =
      numericalColumns.filter(!_.contains("id__"))

    numericalColumnsWithoutIdColumn.foreach(column => {
      runInternal(allColumns, data, spark, column, config)
    })
  }

  private def runInternal(
      allColumns: Array[String],
      data: DataFrame,
      spark: SparkSession,
      column: String,
      config: ExDistADConfig
  ) = {
    val labelColumn = column
    var featureColumns: Array[String] = allColumns
      .filter(!_.equals("s"))
      .filter(!_.equals(labelColumn))
      .filter(p => !containIndex(p, allColumns))
      .filter(p => !p.contains("id__"))

    val o = assembleDF(featureColumns, data)
    val output: DataFrame =
      o._1
    val realColsName: Array[String] = o._2

    output.cache()
    if (config.verbose) {
      LOG.info("Assembled SmartDataframe:")
      output.show(false)
    }
    val treeModel: DecisionTreeRegressionModel =
      trainDecisionTree(output, labelColumn, config)

    val rawRules: List[String] =
      parseDecisionTree(treeModel, output, realColsName)

    var sqlRules: (List[String], List[String]) =
      generateSqlRules(rawRules, realColsName, output)
    output.createOrReplaceTempView("originalData")
//    sqlRules = sortSqlRulesDesc(sqlRules._1)
//    if (config.verbose) {
//      sqlRules._1 foreach println
//    }
    for (i <- 0 until sqlRules._1.length) {
      ruleRunner(
        sqlRules._1(i),
        sqlRules._2(i),
        spark,
        labelColumn,
        config,
        output
      )
    }
//    for (rule <- sqlRules._1) {}

  }

  def ruleRunner(
      rule: String,
      mappedRule: String,
      spark: SparkSession,
      labelColumn: String,
      config: ExDistADConfig,
      output: DataFrame
  ): (Dataset[Row], String) = {
    val fullQuery = "SELECT * FROM originalData WHERE " + rule
    val output1 = spark.sql(fullQuery)
    val result = output1.select(labelColumn)
    import spark.implicits._
    val dataType = labelColumn.split("_(?!__)").last
    var anomalies: Array[Double] = Array()
    // TODO add other numeric datatypes

    var filteredDF = result.filter(!_.anyNull)
    var values: Array[Double] = null
    dataType match {
      case "integer" | "int" =>
        values = filteredDF.map(p => p.getInt(0).toDouble).collect()
      case "decimal" =>
        values = filteredDF.map(p => p.getDouble(0)).collect()
      case "long" =>
        values = filteredDF.map(p => p.getLong(0).toDouble).collect()
      case "double" =>
        values = filteredDF.map(p => p.getDouble(0)).collect()
      case "float" =>
        values = filteredDF.map(p => p.getFloat(0).toDouble).collect()
    }
    anomalies = ExDistADUtil.anomalyDetectionMethod(
      values,
      config
    )
    if (!anomalies.isEmpty) {
      val explanation = "All the values in " + labelColumn + " column are anomaly because of " + mappedRule
      val anomalyList: Dataset[Row] =
        output1.filter(output1(labelColumn).isin(anomalies: _*))

      if (config.verbose) {
        LOG.info(explanation)
        anomalyList.show(false)
      }

      if (config.writeResultToFile) {
        ExDistADUtil.writeToFile(
          config.resultFilePath,
          anomalyList,
          explanation,
          fileCounter
        )
        fileCounter = fileCounter + 1
      }
      return (anomalyList, explanation)
    }
    null
  }
}
