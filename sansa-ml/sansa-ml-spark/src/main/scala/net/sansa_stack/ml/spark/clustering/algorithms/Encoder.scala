package net.sansa_stack.ml.spark.clustering.algorithms

import org.apache.spark.ml.feature.{ VectorAssembler, Word2Vec }
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._


class Encoder {

  /**
   * One hot encoding categorical data
   *
   * @param poiCategories, category ids with corresponding category values
   * @param spark
   * @return one hot encoded DataFrame for each poi
   */
  def oneHotEncoding(poiCategories: RDD[(Long, Set[String])], spark: SparkSession): (DataFrame, Array[Array[Int]]) = {
    // create a set to contain all categories
    var set = scala.collection.mutable.Set[String]()
    // put all categories to set
    poiCategories.collect().foreach(x => x._2.foreach(y => set += y))
    // create columns base on the length of set
    val numPOIS = poiCategories.count().toInt // Array.ofDim only accept Int
    val categoryArray = set.toArray
    val oneHotMatrix = Array.ofDim[Int](numPOIS, categoryArray.length + 1) // one column keep poi id
    // initialize distance matrix, collect first needed
    var i = 0
    poiCategories.collect().foreach(x =>
      {
        oneHotMatrix(i)(0) = x._1.toInt
        for (j <- 1 until categoryArray.length + 1) {
          oneHotMatrix(i)(j) = 0
        }
        x._2.foreach(y =>
          { // encode corresponding category value to 1
            oneHotMatrix(i)(categoryArray.indexOf(y) + 1) = 1
          })
        i += 1
      })
    // vector keep all StructField
    val fields = Array.ofDim[StructField](categoryArray.length + 1)
    val featureColumns = Array.ofDim[String](categoryArray.length + 1)
    // keep other columns with integer type
    for (i <- 0 until categoryArray.length + 1) {
      fields(i) = StructField(i.toString, IntegerType, true)
      featureColumns(i) = i.toString
    }
    val schema = new StructType(fields)
    val oneHotEncodedRDD = spark.sparkContext.parallelize(oneHotMatrix).map(x => Row.fromSeq(x.toList))
    val oneHotEncodedDF = spark.createDataFrame(oneHotEncodedRDD, schema)
    // set up 'features' column
    val assemblerFeatures = new VectorAssembler().setInputCols(featureColumns.slice(1, featureColumns.length)).setOutputCol("features")
    val transformedDf = assemblerFeatures.transform(oneHotEncodedDF)
    (transformedDf, oneHotMatrix)
  }

  /**
   * word2Vec encoding
   *
   * @param poiCategories category ids with corresponding category values
   * @param spark
   * @return word2Vec encoded categories for each poi in DataFrame
   */
  def wordVectorEncoder(poiCategories: RDD[(Long, Set[String])], spark: SparkSession): (DataFrame, RDD[(Int, Array[Double])]) = {
    val word2vec = new Word2Vec().setInputCol("inputCol").setMinCount(1)
    val schema = StructType(StructField("inputCol", ArrayType(StringType, true), true) :: Nil)
    val df = spark.createDataFrame(poiCategories.map(f => Row(f._2.map(x => x.toString).toArray)), schema)
    val wordVectorsRDD = word2vec.fit(df).getVectors.select("word", "vector").rdd
    val vectors = wordVectorsRDD.map(f => (f.getString(0), f.getAs[org.apache.spark.ml.linalg.DenseVector](1)))
    val categoryVectors = vectors.collectAsMap()
    val poiCategoryVectors = poiCategories.map(f => (f._1, f._2.map(x => categoryVectors.get(x).head.toArray)))
    val poiVector = poiCategoryVectors.map(f => (f._1, f._2.size, f._2.toArray.toList.transpose.map(_.sum).toArray))
    val leng = poiVector.take(1)(0)._2
    val poiAvgVector = poiVector.map(x => (x._1.toInt, x._3.map(y => y / x._2)))
    val fields = Array.ofDim[StructField](leng + 1)
    val featureColumns = Array.ofDim[String](leng + 1)
    // keep other columns with integer type
    fields(0) = StructField("id", IntegerType, true)
    featureColumns(0) = "id"
    for (i <- 1 until leng + 1) {
      fields(i) = StructField(i.toString, DoubleType, true)
      featureColumns(i) = i.toString
    }
    val schema2 = new StructType(fields)
    val poiAvgVectorDF = spark.createDataFrame(poiAvgVector.map(x => Row.fromSeq(x._1 +: x._2)), schema2)
    val assemblerFeatures = new VectorAssembler().setInputCols(featureColumns.slice(1, featureColumns.length)).setOutputCol("features")
    val transformedDf = assemblerFeatures.transform(poiAvgVectorDF)
    (transformedDf, poiAvgVector)
  }

  /**
   * multiple dimensional encoding
   *
   * @param distancePairs distance between pair of pois
   * @param numPOIS number of pois
   * @param dimension mapped coordinate dimension
   * @param spark
   * @return encoded coordinates for each poi in DataFrame
   */
  def mdsEncoding(distancePairs: RDD[(Long, Long, Double)], numPOIS: Int, dimension: Int, spark: SparkSession): (DataFrame, Array[(Long, Array[Double])]) = {
    val poi2Coordinates = new MultiDS().multiDimensionScaling(distancePairs, numPOIS, dimension)
    val poi2Coordinates2 = poi2Coordinates.map(x => x._1.toInt :: x._2.toList)
    // create schema
    val fields = Array.ofDim[StructField](dimension + 1)
    val featureColumns = Array.ofDim[String](dimension + 1)
    fields(0) = StructField("id", IntegerType, true)
    featureColumns(0) = "id"
    for (i <- 1 until dimension + 1) {
      fields(i) = StructField(i.toString, DoubleType, true)
      featureColumns(i) = i.toString
    }
    val schema = new StructType(fields)
    val coordinatesRDD = spark.sparkContext.parallelize(poi2Coordinates2.toSeq).map(x => Row.fromSeq(x))
    val coordinatesDF = spark.createDataFrame(coordinatesRDD, schema)
    val assembler = new VectorAssembler().setInputCols(featureColumns.slice(1, featureColumns.length)).setOutputCol("features")
    val featureData = assembler.transform(coordinatesDF)
    (featureData, poi2Coordinates)
  }
}

