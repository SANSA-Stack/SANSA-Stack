package net.sansa_stack.ml.spark.similarity.run

import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, MinHashLSH, Tokenizer}
// import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.functions.udf
import org.apache.spark.mllib.linalg.{Vector, Vectors}

object minHash_rdf_over_text_pipeline {
  def main(args: Array[String]): Unit = {
    run()
  }

  def run(): Unit = {
    println("1. We start Spark session")
    val spark = SparkSession.builder
      .appName(s"MinHash  tryout") // TODO where is this displayed?
      .master("local[*]") // TODO why do we need to specify this?
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // TODO what is this for?
      .getOrCreate()
    println("Spark Session started \n")

    // val input = "/Users/carstendraschner/GitHub/SANSA-ML/sansa-ml-spark/src/main/resources/rdf.nt"

    println("2. Create or load sample RDD Graph")
    val sample_rdd_graph: RDD[Triple] = spark.sparkContext.parallelize(
      Array(
        Triple.create(
          NodeFactory.createURI("exampleUri1"),
          NodeFactory.createURI("examplePredicate1"),
          NodeFactory.createURI("exampleUri2")
        ), Triple.create(
          NodeFactory.createURI("exampleUri2"),
          NodeFactory.createURI("examplePredicate2"),
          NodeFactory.createURI("exampleUri3")
        ), Triple.create(
          NodeFactory.createURI("exampleUri3"),
          NodeFactory.createURI("examplePredicate1"),
          NodeFactory.createURI("exampleUri1")
        ), Triple.create(
          NodeFactory.createURI("exampleUri1"),
          NodeFactory.createURI("examplePredicate1"),
          NodeFactory.createLiteral("example Literal1")
        )
      )
    )

    sample_rdd_graph.foreach(println(_))
    println("Sample RDD Graph created \n")


    println("3. Create or load sample RDD Sub Graph.\nThis is needed for a later recommendation test.")
    val sample_rdd_subgraph: RDD[Triple] = spark.sparkContext.parallelize(
      Array(
        Triple.create(
          NodeFactory.createURI("exampleUri4"),
          NodeFactory.createURI("examplePredicate1"),
          NodeFactory.createURI("exampleUri2")
        ), Triple.create(
          NodeFactory.createURI("exampleUri2"),
          NodeFactory.createURI("examplePredicate2"),
          NodeFactory.createURI("exampleUri4")
        ), Triple.create(
          NodeFactory.createURI("exampleUri4"),
          NodeFactory.createURI("examplePredicate1"),
          NodeFactory.createLiteral("example Literal1")
        )
      )
    )
    sample_rdd_subgraph.foreach(println(_))
    println("Sample Sub RDD Graph created \n")

    println("4. Here we produce a dense transformation into a pseudo tokenized format")
    println("\tin the current case we include relations and nodes connected, but different feature generation are also possible")
    println("\tSo what happens here:\n  " +
      "\t4.1 We transform a triple into two tuples where the first element is subject resp. object and the second element are the predcicates and object concatinated as string.\n" +
      "\t4.2 Then we remove all tuples where the first element (key) is not an URI\n" +
      "\t4.3 Transform key from Node to String\n" +
      "\t4.4 Remove all spaces from features (because some literals have spaces which could lead to misinterpretation in pseudotext tokenization\n" +
      "\t4.5 Group all of the tuples by first element (corresponds to a node centric representation of collected features)\n" +
      "\t4.6 Reduce value information because key was also part of values\n" +
      "\t4.7 From a List of Tokens we create a text by concatinating all pseudo words (representing features)  with inbetween space\n" +
      "\t4.8 Create a Dataframe out of it to align with pipeline requirements")

    val pseudo_text_df = spark.createDataFrame(
      sample_rdd_graph
        .flatMap(t => Seq((t.getSubject, t.getPredicate.toString() + t.getObject.toString()), (t.getObject, t.getPredicate.toString() + t.getSubject.toString()))) // 4.1
        .filter(_._1.isURI) // 4.2
        .map({ case (k, v) => (k.toString(), v) }) // 4.3
        .mapValues(_.replaceAll("\\s", "")) // 4.4
        .groupBy(_._1) // 4.5
        .mapValues(_.map(_._2)) // 4.6
        .map({ case (k, v) => (k, v.reduceLeft(_ + " " + _)) }) // 4.7
        .collect()
        .toSeq
    ).toDF(colNames = "title", "content") // 4.8
    println("This is the resulting DF")
    pseudo_text_df.show()
    println("Transformation to pseudo text DF done.\n")

    println("From here onwoards we follow the ideas of this webpage:\n" +
      "https://databricks.com/de/blog/2017/05/09/detecting-abuse-scale-locality-sensitive-hashing-uber-engineering.html\n")

    println("5. We use the Standart MLlib Tokenizer to create from our pseudotext of features from each node a tokenized DF which has the fitting Data format and types for further MLlib Pipline elements ")
    val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words")
    val wordsDf = tokenizer.transform(pseudo_text_df)
    println("this is how the resulting DF looks like")
    wordsDf.show()
    println("tokenization done!\n")

    println("6. We set up a count Vectorizer which operates on our Tokenized DF")
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(1000000)
      .setMinDF(1) // TODO maybe later higher
      .fit(wordsDf)
    val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 }, DataTypes.BooleanType)
    val vectorizedDf = cvModel.transform(wordsDf).select(col("title"), col("features")) // .filter(isNoneZeroVector(col("features")))
    println("This is how our resulting DF looks like which will be used by the min Hash algo")
    vectorizedDf.show()
    println("Count Vectorization done!\n")


    println("6. Here we finally start with our minhash model\n" +
      "we set number of hash tables\n" +
      "and we decribe the respective columns\n" +
      "then we fit the model to our vectorizedDf\n")
    val mh = new MinHashLSH()
      .setNumHashTables(3)
      .setInputCol("features")
      .setOutputCol("hashValues")
    val model = mh.fit(vectorizedDf)
    println("minHash model fitted!")

    println("8. We have to transform our Dataframe what we want to operate on ")
    model.transform(vectorizedDf).show()
    println("Transformation done")

    /* val sample_key =
    val key = Vectors.sparse(vocabSize, Seq((cvModel.vocabulary.indexOf("united"), 1.0), (cvModel.vocabulary.indexOf("states"), 1.0)))
    val k = 40
    model.approxNearestNeighbors(vectorizedDf, key, k).show() */

    // Self Join
    val threshold = 0.8
    model.approxSimilarityJoin(vectorizedDf, vectorizedDf, threshold).filter("distCol != 0").show()
    spark.stop()

  }
}
