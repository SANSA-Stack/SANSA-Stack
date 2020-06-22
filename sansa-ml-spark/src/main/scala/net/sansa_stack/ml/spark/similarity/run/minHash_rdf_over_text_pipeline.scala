package net.sansa_stack.ml.spark.similarity.run

import net.sansa_stack.rdf.spark.io._

import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.riot.Lang
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, MinHashLSH, Tokenizer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataTypes, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.udf
import java.util.{Calendar, Date}

// import org.apache.spark.mllib.linalg.{Vector, Vectors}

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

    println("2. Create or load sample RDD Graph")

    val input = "/Users/carstendraschner/GitHub/SANSA-ML/sansa-ml-spark/src/main/resources/movie.nt"
    // val input = "/Users/carstendraschner/GitHub/SANSA-ML/sansa-ml-spark/src/main/resources/rdf.nt"
    // val input = "/Users/carstendraschner/Downloads/linkedmdb-18-05-2009-dump_short.nt"

    /* val triples = read_in_nt_triples(
      input = input,
      spark = spark,
      lang = Lang.NTRIPLES
    ) */

    val lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(input)

    val tmp_triples: RDD[Triple] = triples

    /* println("2. Create or load sample RDD Graph")
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


    println("Sample RDD Graph created \n")

    val tmp_triples: RDD[Triple] = triples

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
    println("Sample Sub RDD Graph created \n") */

    println("3. Show RDF Data")
    tmp_triples.foreach(println(_))
    println()

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
      tmp_triples
        .flatMap(t => Seq((t.getSubject, t.getPredicate.toString() + t.getObject.toString()), (t.getObject, t.getSubject.toString() + t.getPredicate.toString()))) // 4.1
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
    pseudo_text_df.show(false)
    println("Transformation to pseudo text DF done.\n")

    // Sure this part of creating artificial text after having some tokens
    // already is not the most efficient way but it makes the comon pipeline
    // usable and we don not need to take care of data types and generation
    // of right Dataframe format

    println("From here onwoards we follow the ideas of this webpage:\n" +
      "https://databricks.com/de/blog/2017/05/09/detecting-abuse-scale-locality-sensitive-hashing-uber-engineering.html\n")

    println("5. We use the Standart MLlib Tokenizer to create from our pseudotext of features from each node a tokenized DF which has the fitting Data format and types for further MLlib Pipline elements ")
    val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words")
    val wordsDf = tokenizer.transform(pseudo_text_df)
    println("this is how the resulting DF looks like")
    wordsDf.show(false)
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
    vectorizedDf.show(false)
    println("Count Vectorization done!\n")


    println("6. Here we finally start with our minhash model\n" +
      "we set number of hash tables\n" +
      "and we decribe the respective columns\n" +
      "then we fit the model to our vectorizedDf\n")
    val mh = new MinHashLSH()
      .setNumHashTables(5)
      .setInputCol("features")
      .setOutputCol("hashValues")
    val model = mh.fit(vectorizedDf)
    println("minHash model fitted!")

    println("8. We have to transform our Dataframe what we want to operate on ")
    model.transform(vectorizedDf).show(false)
    println("Transformation done\n")

    println("9. Perform Similarity Estimations on key and DF by approxNearestNeighbors approx Similarity Join")
    println("\t9.1 The created key is another sample movie which does not exist originally in the dataset")
    println("\t\tWe have to set the vocab-size: " + cvModel.getVocabSize)
    println("\t\tThen our movie has actor a1 with relation acted in ai:")
    println("\t\tAnd is produced by producer p2 over relation pb")
    println("\t\tFinally is published at 20-06-17\n")
    println("\t\tThis is how the new movie key looks like in data frame format\n")

    val key_movie = cvModel.transform(
      tokenizer.transform(
        spark.createDataFrame(
          spark.sparkContext.parallelize(
            Seq(
              Row(
                "m4",
                "file:///Users/carstendraschner/GitHub/SANSA-ML/a1file:///Users/carstendraschner/GitHub/SANSA-ML/ai".toString + " " +
                "file:///Users/carstendraschner/GitHub/SANSA-ML/pbfile:///Users/carstendraschner/GitHub/SANSA-ML/p2".toString + " " +
                """file:///Users/carstendraschner/GitHub/SANSA-ML/pd"2020-06-17"""".toString
              )
            )
          ),
          StructType(
            List(
              StructField("title", StringType, true),
              StructField("content", StringType, true)
            )
          )
        )
      )
    )
    key_movie.show()
    // TODO get sparse vector out of cell features, some zip is also needed to in the end ket a desired key
    val key = Vectors.sparse(80, Seq((4, 1.0), (12, 1.0)))

    /* println(cvModel.vocabulary.indexOf("file:///Users/carstendraschner/GitHub/SANSA-ML/aifile:///Users/carstendraschner/GitHub/SANSA-ML/a1".toString))
    println(cvModel.vocabulary.indexOf("file:///Users/carstendraschner/GitHub/SANSA-ML/aifile:///Users/carstendraschner/GitHub/SANSA-ML/a3".toString))
    println(cvModel.vocabulary.indexOf("file:///Users/carstendraschner/GitHub/SANSA-ML/pbfile:///Users/carstendraschner/GitHub/SANSA-ML/p2".toString))


    val not_working_key = Vectors.sparse(
      cvModel.getVocabSize,
      Seq(
        (cvModel.vocabulary.indexOf("file:///Users/carstendraschner/GitHub/SANSA-ML/aifile:///Users/carstendraschner/GitHub/SANSA-ML/a1".toString), 1.0),
        (cvModel.vocabulary.indexOf("file:///Users/carstendraschner/GitHub/SANSA-ML/aifile:///Users/carstendraschner/GitHub/SANSA-ML/a3".toString), 1.0),
        (cvModel.vocabulary.indexOf("file:///Users/carstendraschner/GitHub/SANSA-ML/pbfile:///Users/carstendraschner/GitHub/SANSA-ML/p2".toString), 1.0)
      )
    ) */
    val k = 40
    print("ApproxNearestNeighbors")
    model.approxNearestNeighbors(vectorizedDf, key, k).show(false)

    // Self Join
    println("Approx Similarity Join")
    val threshold = 0.8
    val minhashed_df = model.approxSimilarityJoin(vectorizedDf, vectorizedDf, threshold) // .filter("distCol != 0")
    minhashed_df.show(false)
    minhashed_df.show()
    println("minHash similarity Join has been Performed")

    println("10 Experiment Graph Creation:\n" +
      "\tin this part we create from our results an rdf graph which represents the gained information\n" +
      "\tWith this approach we might be able to reuse the gained information in future approaches")

    /*

    val experiment_name = "Spark_Min_Hash"
    val experiment_type = "Sematic Similarity Estimation"
    val evaluation_datetime = Calendar.getInstance().getTime().toString // todo quick and dirty hack with tostring
    val measure_type = "distance"

    // minhashed_df.withColumn("uriA", col("datasetA").getField("title")).show(false)

    /* .collect()
    .map(_.getAs[]).
    foreach(println(_)) */

    import spark.implicits._

    def experiment_to_triples(
        ea: String,
        eb: String,
        value: Double,
        datetime: String,
        exp_type: String,
        exp_name: String,
        measure_type: String
      ): List[Triple] = {

        val a: String = ea.split("/").last
        val b: String = eb.split("/").last

        // relations
        val er = "element"
        val vr = "value"
        val extr = "experiment_type"
        val exnr = "experiment_name"
        val mtr = "experiment_measurement_type"
        val dtr = "experiment_datetime"

        // central node uri
        val cnu: String = exp_type + " - " + exp_name + " - " + a + " - " + b + " - " + datetime

        /* Array(
          Array(a, er, cnu),
          Array(b, er, cnu)
        )
        */
        List(
          Triple.create(
            NodeFactory.createURI(a),
            NodeFactory.createURI(er),
            NodeFactory.createURI(cnu)
          ), Triple.create(
            NodeFactory.createURI(b),
            NodeFactory.createURI(er),
            NodeFactory.createURI(cnu)
          ), Triple.create(
            NodeFactory.createURI(cnu),
            NodeFactory.createURI(vr),
            NodeFactory.createLiteral(value.toString)
          ), Triple.create(
            NodeFactory.createURI(cnu),
            NodeFactory.createURI(extr),
            NodeFactory.createLiteral(experiment_type)
          ), Triple.create(
            NodeFactory.createURI(cnu),
            NodeFactory.createURI(exnr),
            NodeFactory.createLiteral(experiment_name)
          ), Triple.create(
            NodeFactory.createURI(cnu),
            NodeFactory.createURI(dtr),
            NodeFactory.createLiteral(evaluation_datetime)
          )
        )
      }



    import org.apache.spark.sql.functions.{col, lit}

    val er = "element"
    val vr = "value"
    val extr = "experiment_type"
    val exnr = "experiment_name"
    val mtr = "experiment_measurement_type"
    val dtr = "experiment_datetime"

    val experiment_name = "Spark_Min_Hash"
    val experiment_type = "Sematic Similarity Estimation"
    val evaluation_datetime = Calendar.getInstance().getTime().toString // todo quick and dirty hack with tostring
    val measure_type = "distance" */


    val experiment_results: RDD[Triple] = minhashed_df
      .withColumn("ea", col("datasetA").getField("title")) // rdd
      .withColumn("eb", col("datasetB").getField("title"))
      .select("ea", "eb", "distCol")
      .rdd
      .flatMap(
        row => {
          // Strings for relation names
          val er = "element"
          val vr = "value"
          val extr = "experiment_type"
          val exnr = "experiment_name"
          val mtr = "experiment_measurement_type"
          val dtr = "experiment_datetime"

          // Strings for uris adn literals
          val experiment_name = "Spark_Min_Hash"
          val experiment_type = "Sematic Similarity Estimation"
          val evaluation_datetime = Calendar.getInstance().getTime().toString // todo quick and dirty hack with tostring
          val measure_type = "distance"

          val a = row(0).toString().split("/").last
          val b = row(1).toString().split("/").last
          val value = row(2).toString
          val cnu: String = experiment_type + " - " + experiment_name + " - " + a + " - " + b + " - " + evaluation_datetime

          List(
            Triple.create(
              NodeFactory.createURI(a),
              NodeFactory.createURI(er),
              NodeFactory.createURI(cnu)
            ), Triple.create(
              NodeFactory.createURI(b),
              NodeFactory.createURI(er),
              NodeFactory.createURI(cnu)
            ), Triple.create(
              NodeFactory.createURI(cnu),
              NodeFactory.createURI(vr),
              NodeFactory.createLiteral(value.toString)
            ), Triple.create(
              NodeFactory.createURI(cnu),
              NodeFactory.createURI(extr),
              NodeFactory.createLiteral(experiment_type)
            ), Triple.create(
              NodeFactory.createURI(cnu),
              NodeFactory.createURI(exnr),
              NodeFactory.createLiteral(experiment_name)
            ), Triple.create(
              NodeFactory.createURI(cnu),
              NodeFactory.createURI(dtr),
              NodeFactory.createLiteral(evaluation_datetime)
            )
          )
        }
      )
    experiment_results.foreach(println(_))

    // TODO check whether this should be always public visible
    val experiment_name = "Spark_Min_Hash"
    val experiment_type = "Sematic Similarity Estimation"
    val evaluation_datetime = Calendar.getInstance().getTime().toString // todo quick and dirty hack with tostring
    val measure_type = "distance"

    val experiment_hash: String = experiment_type + " - " + experiment_name + " - " + evaluation_datetime


    val output = "/Users/carstendraschner/Downloads/experiment_results_" + experiment_hash + ".nt"
    // val output = "/Users/carstendraschner/Downloads/experiment_results"

    experiment_results.saveAsNTriplesFile(output)

    println("Triples are generated and stored in output path:")
    println(output)

    spark.stop()
  }
}
