package net.sansa_stack.ml.spark.explainableanomalydetection

import com.holdenkarau.spark.testing.SharedSparkContext
import net.sansa_stack.ml.spark.explainableanomalydetection.ExDistAD.{
  assembleDF,
  containIndex,
  generateSqlRules,
  parseDecisionTree,
  ruleRunner,
  trainDecisionTree
}
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

/**
  * Test class for @link{ExDistAD}
  */
class ExDistADTest extends AnyFunSuite with SharedSparkContext {

  System.setProperty(
    "spark.serializer",
    "org.apache.spark.serializer.KryoSerializer"
  )
  System.setProperty(
    "spark.kryo.registrator",
    "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator"
  )

  lazy val spark = SparkSession
    .builder()
    .appName(s"SparqlFrame Transformer Unit Test")
    .config(
      "spark.serializer",
      "org.apache.spark.serializer.KryoSerializer"
    ) // we need Kryo serialization enabled with some custom serializers
    .config(
      "spark.kryo.registrator",
      String.join(
        ", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"
      )
    )
    .config("spark.sql.crossJoin.enabled", true)
    .getOrCreate()

  private val configFilePath =
    this.getClass.getClassLoader.getResource("ExDistAD/config.conf").getPath

  override def beforeAll() {
    super.beforeAll()
    JenaSystem.init()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.close()
  }

  test("Read Data Test") {
    val config: ExDistADConfig = new ExDistADConfig(configFilePath)
    val dataPath =
      this.getClass.getClassLoader.getResource(config.inputData).getPath
    val data = ExDistADUtil.readData(spark, dataPath)
    assert(data.count() == 100100)
  }

  test("Smart Data Frame Test") {
    val config: ExDistADConfig = new ExDistADConfig(configFilePath)
    val dataPath =
      this.getClass.getClassLoader.getResource(config.inputData).getPath
    val originalDataRDD = ExDistADUtil.readData(spark, dataPath)
    val data = ExSmartDataFrame.transform(originalDataRDD, config)
    assert(data.count() == 20020)
  }

  test("Assemble DF Test") {
    val config: ExDistADConfig = new ExDistADConfig(configFilePath)
    val dataPath =
      this.getClass.getClassLoader.getResource(config.inputData).getPath
    val originalDataRDD = ExDistADUtil.readData(spark, dataPath)
    val data = ExSmartDataFrame.transform(originalDataRDD, config)
    val allColumns: Array[String] = data.columns
    val numericalColumns =
      allColumns
        .filter(
          p =>
            p.endsWith("__int") || p.endsWith("__integer") || p.endsWith(
              "__decimal"
            )
        )
    val numericalColumnsWithoutIdColumn: Array[String] =
      numericalColumns.filter(!_.contains("id__"))

    val labelColumn = "age__integer"
    var featureColumns: Array[String] = allColumns
      .filter(!_.equals("s"))
      .filter(!_.equals(labelColumn))
      .filter(p => !containIndex(p, allColumns))
      .filter(p => !p.contains("id__"))

    val o = assembleDF(featureColumns, data)
    assert(o._1.columns.contains("indexedFeatures"))
  }

  test("Decision Tree Test") {
    val config: ExDistADConfig = new ExDistADConfig(configFilePath)
    val dataPath =
      this.getClass.getClassLoader.getResource(config.inputData).getPath
    val originalDataRDD = ExDistADUtil.readData(spark, dataPath)
    val data = ExSmartDataFrame.transform(originalDataRDD, config)
    val allColumns: Array[String] = data.columns
    val numericalColumns =
      allColumns
        .filter(
          p =>
            p.endsWith("__int") || p.endsWith("__integer") || p.endsWith(
              "__decimal"
            )
        )
    val numericalColumnsWithoutIdColumn: Array[String] =
      numericalColumns.filter(!_.contains("id__"))

    val labelColumn = "age__integer"
    var featureColumns: Array[String] = allColumns
      .filter(!_.equals("s"))
      .filter(!_.equals(labelColumn))
      .filter(p => !containIndex(p, allColumns))
      .filter(p => !p.contains("id__"))

    val o = assembleDF(featureColumns, data)
    assert(o._1.columns.contains("indexedFeatures"))

    val treeModel: DecisionTreeRegressionModel =
      trainDecisionTree(o._1, labelColumn, config)
    assert(treeModel.numNodes == 7)
  }

  test("Parse Decision Tree Test") {
    val config: ExDistADConfig = new ExDistADConfig(configFilePath)
    val dataPath =
      this.getClass.getClassLoader.getResource(config.inputData).getPath
    val originalDataRDD = ExDistADUtil.readData(spark, dataPath)
    val data = ExSmartDataFrame.transform(originalDataRDD, config)
    val allColumns: Array[String] = data.columns
    val numericalColumns =
      allColumns
        .filter(
          p =>
            p.endsWith("__int") || p.endsWith("__integer") || p.endsWith(
              "__decimal"
            )
        )
    val numericalColumnsWithoutIdColumn: Array[String] =
      numericalColumns.filter(!_.contains("id__"))

    val labelColumn = "age__integer"
    var featureColumns: Array[String] = allColumns
      .filter(!_.equals("s"))
      .filter(!_.equals(labelColumn))
      .filter(p => !containIndex(p, allColumns))
      .filter(p => !p.contains("id__"))

    val o = assembleDF(featureColumns, data)
    assert(o._1.columns.contains("indexedFeatures"))

    val treeModel: DecisionTreeRegressionModel =
      trainDecisionTree(o._1, labelColumn, config)

    val rawRules: List[String] =
      parseDecisionTree(treeModel, o._1, o._2)
    assert(rawRules.length == 6)
  }

  test("SQL Generator Test") {
    val config: ExDistADConfig = new ExDistADConfig(configFilePath)
    val dataPath =
      this.getClass.getClassLoader.getResource(config.inputData).getPath
    val originalDataRDD = ExDistADUtil.readData(spark, dataPath)
    val data = ExSmartDataFrame.transform(originalDataRDD, config)
    val allColumns: Array[String] = data.columns
    val numericalColumns =
      allColumns
        .filter(
          p =>
            p.endsWith("__int") || p.endsWith("__integer") || p.endsWith(
              "__decimal"
            )
        )
    val numericalColumnsWithoutIdColumn: Array[String] =
      numericalColumns.filter(!_.contains("id__"))

    val labelColumn = "age__integer"
    var featureColumns: Array[String] = allColumns
      .filter(!_.equals("s"))
      .filter(!_.equals(labelColumn))
      .filter(p => !containIndex(p, allColumns))
      .filter(p => !p.contains("id__"))

    val o = assembleDF(featureColumns, data)
    assert(o._1.columns.contains("indexedFeatures"))

    val treeModel: DecisionTreeRegressionModel =
      trainDecisionTree(o._1, labelColumn, config)

    val rawRules: List[String] =
      parseDecisionTree(treeModel, o._1, o._2)
    var sqlRules: (List[String], List[String]) =
      generateSqlRules(rawRules, o._2, o._1)
    assert(sqlRules._1.length == 6)
  }

  test("Anomaly Detection") {
    val config: ExDistADConfig = new ExDistADConfig(configFilePath)
    val dataPath =
      this.getClass.getClassLoader.getResource(config.inputData).getPath
    val originalDataRDD = ExDistADUtil.readData(spark, dataPath)
    val data = ExSmartDataFrame.transform(originalDataRDD, config)
    val allColumns: Array[String] = data.columns
    val numericalColumns =
      allColumns
        .filter(
          p =>
            p.endsWith("__int") || p.endsWith("__integer") || p.endsWith(
              "__decimal"
            )
        )
    val numericalColumnsWithoutIdColumn: Array[String] =
      numericalColumns.filter(!_.contains("id__"))

    val labelColumn = "age__integer"
    var featureColumns: Array[String] = allColumns
      .filter(!_.equals("s"))
      .filter(!_.equals(labelColumn))
      .filter(p => !containIndex(p, allColumns))
      .filter(p => !p.contains("id__"))

    val o = assembleDF(featureColumns, data)
    o._1.createOrReplaceTempView("originalData")

    val treeModel: DecisionTreeRegressionModel =
      trainDecisionTree(o._1, labelColumn, config)

    val rawRules: List[String] =
      parseDecisionTree(treeModel, o._1, o._2)
    var sqlRules: (List[String], List[String]) =
      generateSqlRules(rawRules, o._2, o._1)
    var anomalies = ruleRunner(
      sqlRules._1(0),
      sqlRules._2(0),
      spark,
      labelColumn,
      config,
      o._1
    )
    assert(anomalies._1.count() == 5)
  }
}
