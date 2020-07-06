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
import org.apache.spark.sql.functions._

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

    import spark.implicits._ // TODO does anyone know for which purposes we need

    println("Spark Session started \n")

    println("2. Create or load sample RDD Graph")

    /*
    param:
    input:String this is the filepath from where we want to load our rdf data
    lang:   currentlYy only nt is supported. usually this parameter is hard coded set for this rdf layer
     */

    val input = "/Users/carstendraschner/GitHub/SANSA-ML/sansa-ml-spark/src/main/resources/movie.nt"
    // val input = "/Users/carstendraschner/GitHub/SANSA-ML/sansa-ml-spark/src/main/resources/rdf.nt"
    // val input = "/Users/carstendraschner/Downloads/linkedmdb-18-05-2009-dump_short.nt"

    /*
      // TODO why is this also an option and where is the difference to the second one
      val triples = read_in_nt_triples(
      input = input,
      spark = spark,
      lang = Lang.NTRIPLES
    ) */

    val lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(input)

    val tmp_triples: RDD[Triple] = triples

    /*
    output:
    RDD[Triple] this is the output of this step. it is aligned with the sansa rdf layer procedure.
     */

    println("3. Show RDF Data")
    tmp_triples.foreach(println(_))
    println()

    /*
    output:
    file:///Users/carstendraschner/GitHub/SANSA-ML/a1 @file:///Users/carstendraschner/GitHub/SANSA-ML/ai file:///Users/carstendraschner/GitHub/SANSA-ML/m1
    file:///Users/carstendraschner/GitHub/SANSA-ML/m1 @file:///Users/carstendraschner/GitHub/SANSA-ML/pd "1989-11-9"
    file:///Users/carstendraschner/GitHub/SANSA-ML/m1 @file:///Users/carstendraschner/GitHub/SANSA-ML/pb file:///Users/carstendraschner/GitHub/SANSA-ML/p1
    file:///Users/carstendraschner/GitHub/SANSA-ML/a2 @file:///Users/carstendraschner/GitHub/SANSA-ML/ai file:///Users/carstendraschner/GitHub/SANSA-ML/m1
    file:///Users/carstendraschner/GitHub/SANSA-ML/a2 @file:///Users/carstendraschner/GitHub/SANSA-ML/ai file:///Users/carstendraschner/GitHub/SANSA-ML/m2
    file:///Users/carstendraschner/GitHub/SANSA-ML/m2 @file:///Users/carstendraschner/GitHub/SANSA-ML/pb file:///Users/carstendraschner/GitHub/SANSA-ML/p2
    file:///Users/carstendraschner/GitHub/SANSA-ML/a3 @file:///Users/carstendraschner/GitHub/SANSA-ML/ai file:///Users/carstendraschner/GitHub/SANSA-ML/m2
    file:///Users/carstendraschner/GitHub/SANSA-ML/m2 @file:///Users/carstendraschner/GitHub/SANSA-ML/pd "1999-01-01"
    file:///Users/carstendraschner/GitHub/SANSA-ML/a4 @file:///Users/carstendraschner/GitHub/SANSA-ML/ai file:///Users/carstendraschner/GitHub/SANSA-ML/m2
    file:///Users/carstendraschner/GitHub/SANSA-ML/a4 @file:///Users/carstendraschner/GitHub/SANSA-ML/ai file:///Users/carstendraschner/GitHub/SANSA-ML/m3
    file:///Users/carstendraschner/GitHub/SANSA-ML/m3 @file:///Users/carstendraschner/GitHub/SANSA-ML/pd "2004-12-24"
    file:///Users/carstendraschner/GitHub/SANSA-ML/m3 @file:///Users/carstendraschner/GitHub/SANSA-ML/pb file:///Users/carstendraschner/GitHub/SANSA-ML/p2
    */


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

    /*
    param input:
    read in RDD[Triple]
    mode:string a set of possible 12 modes for feature extraction is made available
    column names: Strings. we should be able to set these in future
     */

    // TODO this need to be put in a method or a class which handles different modes and further options in a concise way
    // it hasn't been done for this script to have a working pipeline which includes everything to discuss how modularity should be factored
    val pseudo_text_df = spark.createDataFrame(
      tmp_triples
        // mode at
        .flatMap(t => Seq((t.getSubject, t.getPredicate.toString() + t.getObject.toString()), (t.getObject, t.getSubject.toString() + t.getPredicate.toString()))) // 4.1
        // mode it
        // .flatMap(t => Seq((t.getObject, t.getSubject.toString() + t.getPredicate.toString()))) // 4.1
        // mode ot
        // .flatMap(t => Seq((t.getSubject, t.getPredicate.toString() + t.getObject.toString()))) // 4.1
        // mode ar
        // .flatMap(t => Seq((t.getSubject, t.getPredicate.toString()), (t.getObject, t.getPredicate.toString()))) // 4.1
        // mode ir
        // .flatMap(t => Seq((t.getObject, t.getPredicate.toString()))) // 4.1
        // mode or
        // .flatMap(t => Seq((t.getSubject, t.getPredicate.toString()))) // 4.1
        // mode an
        // .flatMap(t => Seq((t.getSubject, t.getObject.toString()), (t.getObject, t.getSubject.toString()))) // 4.1
        // mode in
        // .flatMap(t => Seq((t.getObject, t.getSubject.toString()))) // 4.1
        // mode on
        // .flatMap(t => Seq((t.getSubject, t.getObject.toString()))) // 4.1
        // mode as
        // .flatMap(t => Seq((t.getSubject, t.getObject.toString()), (t.getObject, t.getSubject.toString()), (t.getSubject, t.getPredicate.toString()), (t.getObject, t.getPredicate.toString()))) // 4.1
        // mode is
        // .flatMap(t => Seq((t.getObject, t.getSubject.toString()), (t.getObject, t.getPredicate.toString()))) // 4.1
        // mode os
        // .flatMap(t => Seq((t.getSubject, t.getObject.toString()), (t.getSubject, t.getPredicate.toString()))) // 4.1
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

    /*
    output:
    spark Dataframe with columns title and content, title is the uri as string after read in and content are a string of all extracted features concatinated
     */

    /*
    output:
    +-------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |title                                            |content                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
    +-------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m1|file:///Users/carstendraschner/GitHub/SANSA-ML/a1file:///Users/carstendraschner/GitHub/SANSA-ML/ai file:///Users/carstendraschner/GitHub/SANSA-ML/pbfile:///Users/carstendraschner/GitHub/SANSA-ML/p1 file:///Users/carstendraschner/GitHub/SANSA-ML/pd"1989-11-9" file:///Users/carstendraschner/GitHub/SANSA-ML/a2file:///Users/carstendraschner/GitHub/SANSA-ML/ai                                                                                                    |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m2|file:///Users/carstendraschner/GitHub/SANSA-ML/a2file:///Users/carstendraschner/GitHub/SANSA-ML/ai file:///Users/carstendraschner/GitHub/SANSA-ML/pbfile:///Users/carstendraschner/GitHub/SANSA-ML/p2 file:///Users/carstendraschner/GitHub/SANSA-ML/pd"1999-01-01" file:///Users/carstendraschner/GitHub/SANSA-ML/a3file:///Users/carstendraschner/GitHub/SANSA-ML/ai file:///Users/carstendraschner/GitHub/SANSA-ML/a4file:///Users/carstendraschner/GitHub/SANSA-ML/ai|
    |file:///Users/carstendraschner/GitHub/SANSA-ML/a1|file:///Users/carstendraschner/GitHub/SANSA-ML/aifile:///Users/carstendraschner/GitHub/SANSA-ML/m1                                                                                                                                                                                                                                                                                                                                                                       |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m3|file:///Users/carstendraschner/GitHub/SANSA-ML/a4file:///Users/carstendraschner/GitHub/SANSA-ML/ai file:///Users/carstendraschner/GitHub/SANSA-ML/pd"2004-12-24" file:///Users/carstendraschner/GitHub/SANSA-ML/pbfile:///Users/carstendraschner/GitHub/SANSA-ML/p2                                                                                                                                                                                                      |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/a2|file:///Users/carstendraschner/GitHub/SANSA-ML/aifile:///Users/carstendraschner/GitHub/SANSA-ML/m1 file:///Users/carstendraschner/GitHub/SANSA-ML/aifile:///Users/carstendraschner/GitHub/SANSA-ML/m2                                                                                                                                                                                                                                                                    |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/a3|file:///Users/carstendraschner/GitHub/SANSA-ML/aifile:///Users/carstendraschner/GitHub/SANSA-ML/m2                                                                                                                                                                                                                                                                                                                                                                       |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/a4|file:///Users/carstendraschner/GitHub/SANSA-ML/aifile:///Users/carstendraschner/GitHub/SANSA-ML/m2 file:///Users/carstendraschner/GitHub/SANSA-ML/aifile:///Users/carstendraschner/GitHub/SANSA-ML/m3                                                                                                                                                                                                                                                                    |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/p1|file:///Users/carstendraschner/GitHub/SANSA-ML/m1file:///Users/carstendraschner/GitHub/SANSA-ML/pb                                                                                                                                                                                                                                                                                                                                                                       |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/p2|file:///Users/carstendraschner/GitHub/SANSA-ML/m2file:///Users/carstendraschner/GitHub/SANSA-ML/pb file:///Users/carstendraschner/GitHub/SANSA-ML/m3file:///Users/carstendraschner/GitHub/SANSA-ML/pb                                                                                                                                                                                                                                                                    |
    +-------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */

    // Sure this part of creating artificial text after having some tokens
    // already is not the most efficient way but it makes the comon pipeline
    // usable and we don not need to take care of data types and generation
    // of right Dataframe format

    println("From here onwoards we follow the ideas of this webpage:\n" +
      "https://databricks.com/de/blog/2017/05/09/detecting-abuse-scale-locality-sensitive-hashing-uber-engineering.html\n")

    println("5. We use the Standart MLlib Tokenizer to create from our pseudotext of features from each node a tokenized DF which has the fitting Data format and types for further MLlib Pipline elements ")

    /*
    input dataframe with two columns, one for title one for content
    title column name
    content column name
    name of words output column
     */

    val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words")
    val wordsDf = tokenizer.transform(pseudo_text_df)
    println("this is how the resulting DF looks like")
    wordsDf.show(false)
    println("tokenization done!\n")

    /*
    dataframe with three columns title content words, words is a list of strings
     */

    /*
    output:
    +-------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |title                                            |content                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |words                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
    +-------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m1|file:///Users/carstendraschner/GitHub/SANSA-ML/a1file:///Users/carstendraschner/GitHub/SANSA-ML/ai file:///Users/carstendraschner/GitHub/SANSA-ML/pbfile:///Users/carstendraschner/GitHub/SANSA-ML/p1 file:///Users/carstendraschner/GitHub/SANSA-ML/pd"1989-11-9" file:///Users/carstendraschner/GitHub/SANSA-ML/a2file:///Users/carstendraschner/GitHub/SANSA-ML/ai                                                                                                    |[file:///users/carstendraschner/github/sansa-ml/a1file:///users/carstendraschner/github/sansa-ml/ai, file:///users/carstendraschner/github/sansa-ml/pbfile:///users/carstendraschner/github/sansa-ml/p1, file:///users/carstendraschner/github/sansa-ml/pd"1989-11-9", file:///users/carstendraschner/github/sansa-ml/a2file:///users/carstendraschner/github/sansa-ml/ai]                                                                                                     |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m2|file:///Users/carstendraschner/GitHub/SANSA-ML/a2file:///Users/carstendraschner/GitHub/SANSA-ML/ai file:///Users/carstendraschner/GitHub/SANSA-ML/pbfile:///Users/carstendraschner/GitHub/SANSA-ML/p2 file:///Users/carstendraschner/GitHub/SANSA-ML/pd"1999-01-01" file:///Users/carstendraschner/GitHub/SANSA-ML/a3file:///Users/carstendraschner/GitHub/SANSA-ML/ai file:///Users/carstendraschner/GitHub/SANSA-ML/a4file:///Users/carstendraschner/GitHub/SANSA-ML/ai|[file:///users/carstendraschner/github/sansa-ml/a2file:///users/carstendraschner/github/sansa-ml/ai, file:///users/carstendraschner/github/sansa-ml/pbfile:///users/carstendraschner/github/sansa-ml/p2, file:///users/carstendraschner/github/sansa-ml/pd"1999-01-01", file:///users/carstendraschner/github/sansa-ml/a3file:///users/carstendraschner/github/sansa-ml/ai, file:///users/carstendraschner/github/sansa-ml/a4file:///users/carstendraschner/github/sansa-ml/ai]|
    |file:///Users/carstendraschner/GitHub/SANSA-ML/a1|file:///Users/carstendraschner/GitHub/SANSA-ML/aifile:///Users/carstendraschner/GitHub/SANSA-ML/m1                                                                                                                                                                                                                                                                                                                                                                       |[file:///users/carstendraschner/github/sansa-ml/aifile:///users/carstendraschner/github/sansa-ml/m1]                                                                                                                                                                                                                                                                                                                                                                           |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m3|file:///Users/carstendraschner/GitHub/SANSA-ML/a4file:///Users/carstendraschner/GitHub/SANSA-ML/ai file:///Users/carstendraschner/GitHub/SANSA-ML/pd"2004-12-24" file:///Users/carstendraschner/GitHub/SANSA-ML/pbfile:///Users/carstendraschner/GitHub/SANSA-ML/p2                                                                                                                                                                                                      |[file:///users/carstendraschner/github/sansa-ml/a4file:///users/carstendraschner/github/sansa-ml/ai, file:///users/carstendraschner/github/sansa-ml/pd"2004-12-24", file:///users/carstendraschner/github/sansa-ml/pbfile:///users/carstendraschner/github/sansa-ml/p2]                                                                                                                                                                                                        |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/a2|file:///Users/carstendraschner/GitHub/SANSA-ML/aifile:///Users/carstendraschner/GitHub/SANSA-ML/m1 file:///Users/carstendraschner/GitHub/SANSA-ML/aifile:///Users/carstendraschner/GitHub/SANSA-ML/m2                                                                                                                                                                                                                                                                    |[file:///users/carstendraschner/github/sansa-ml/aifile:///users/carstendraschner/github/sansa-ml/m1, file:///users/carstendraschner/github/sansa-ml/aifile:///users/carstendraschner/github/sansa-ml/m2]                                                                                                                                                                                                                                                                       |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/a3|file:///Users/carstendraschner/GitHub/SANSA-ML/aifile:///Users/carstendraschner/GitHub/SANSA-ML/m2                                                                                                                                                                                                                                                                                                                                                                       |[file:///users/carstendraschner/github/sansa-ml/aifile:///users/carstendraschner/github/sansa-ml/m2]                                                                                                                                                                                                                                                                                                                                                                           |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/a4|file:///Users/carstendraschner/GitHub/SANSA-ML/aifile:///Users/carstendraschner/GitHub/SANSA-ML/m2 file:///Users/carstendraschner/GitHub/SANSA-ML/aifile:///Users/carstendraschner/GitHub/SANSA-ML/m3                                                                                                                                                                                                                                                                    |[file:///users/carstendraschner/github/sansa-ml/aifile:///users/carstendraschner/github/sansa-ml/m2, file:///users/carstendraschner/github/sansa-ml/aifile:///users/carstendraschner/github/sansa-ml/m3]                                                                                                                                                                                                                                                                       |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/p1|file:///Users/carstendraschner/GitHub/SANSA-ML/m1file:///Users/carstendraschner/GitHub/SANSA-ML/pb                                                                                                                                                                                                                                                                                                                                                                       |[file:///users/carstendraschner/github/sansa-ml/m1file:///users/carstendraschner/github/sansa-ml/pb]                                                                                                                                                                                                                                                                                                                                                                           |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/p2|file:///Users/carstendraschner/GitHub/SANSA-ML/m2file:///Users/carstendraschner/GitHub/SANSA-ML/pb file:///Users/carstendraschner/GitHub/SANSA-ML/m3file:///Users/carstendraschner/GitHub/SANSA-ML/pb                                                                                                                                                                                                                                                                    |[file:///users/carstendraschner/github/sansa-ml/m2file:///users/carstendraschner/github/sansa-ml/pb, file:///users/carstendraschner/github/sansa-ml/m3file:///users/carstendraschner/github/sansa-ml/pb]                                                                                                                                                                                                                                                                       |
    +-------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */

    println("6. We set up a count Vectorizer which operates on our Tokenized DF")

    /*
    dataframe with title and words column
    input column name
    output column name
    vocal size
    minDF for minimum number occurences to be taken into account
     */

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

    /*
    output dataframe with title and features where features are Vector
     */

    /*
    output:
    +-------------------------------------------------+---------------------------------------+
    |title                                            |features                               |
    +-------------------------------------------------+---------------------------------------+
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m1|(15,[1,5,9,11],[1.0,1.0,1.0,1.0])      |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m2|(15,[1,3,4,6,13],[1.0,1.0,1.0,1.0,1.0])|
    |file:///Users/carstendraschner/GitHub/SANSA-ML/a1|(15,[2],[1.0])                         |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m3|(15,[3,4,14],[1.0,1.0,1.0])            |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/a2|(15,[0,2],[1.0,1.0])                   |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/a3|(15,[0],[1.0])                         |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/a4|(15,[0,12],[1.0,1.0])                  |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/p1|(15,[8],[1.0])                         |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/p2|(15,[7,10],[1.0,1.0])                  |
    +-------------------------------------------------+---------------------------------------+
    */


    println("7. Here we finally start with our minhash model\n" +
      "\t- we set number of hash tables\n" +
      "\t- and we decribe the respective columns\n" +
      "\t- then we fit the model to our vectorizedDf\n")

    /*
    input col: String
    output col: String
    numHashTables: Int
     */

    val mh = new MinHashLSH()
      .setNumHashTables(5)
      .setInputCol("features")
      .setOutputCol("hashValues")
    val model = mh.fit(vectorizedDf)
    println("minHash model fitted!")

    /*
    output
    fitted model
     */

    println("8. We have to transform our Dataframe what we want to operate on ")

    /*
    tranformed df which has now the hashed column
     */
    model.transform(vectorizedDf).show(false)
    println("Transformation done\n")

    /*
    output:
    +-------------------------------------------------+---------------------------------------+-----------------------------------------------------------------------------------+
    |title                                            |features                               |hashValues                                                                         |
    +-------------------------------------------------+---------------------------------------+-----------------------------------------------------------------------------------+
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m1|(15,[1,5,9,11],[1.0,1.0,1.0,1.0])      |[[4.4610989E8], [2.82022781E8], [3.13658409E8], [3.37005419E8], [9.2446381E7]]     |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m2|(15,[1,3,4,6,13],[1.0,1.0,1.0,1.0,1.0])|[[3.54796773E8], [2.82022781E8], [4.76528358E8], [7.12958632E8], [1.64558731E8]]   |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/a1|(15,[2],[1.0])                         |[[2.25592966E8], [4.98143035E8], [1.770683816E9], [1.247220523E9], [1.702128832E9]]|
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m3|(15,[3,4,14],[1.0,1.0,1.0])            |[[9.78456855E8], [7.14263289E8], [4.76528358E8], [1.623173736E9], [1.64558731E8]]  |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/a2|(15,[0,2],[1.0,1.0])                   |[[2.25592966E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [7.01119548E8]]    |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/a3|(15,[0],[1.0])                         |[[7.57939931E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [7.01119548E8]]    |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/a4|(15,[0,12],[1.0,1.0])                  |[[7.57939931E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [5.92951023E8]]    |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/p1|(15,[8],[1.0])                         |[[6.66626814E8], [1.794864559E9], [1.19975297E8], [1.464865058E9], [6.29007198E8]] |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/p2|(15,[7,10],[1.0,1.0])                  |[[1.34279849E8], [1.89030324E8], [1.414130755E9], [1.78696741E8], [1.28502556E8]]  |
    +-------------------------------------------------+---------------------------------------+-----------------------------------------------------------------------------------+
     */

    println("9. Perform Similarity Estimations on key and DF by approxNearestNeighbors approx Similarity Join")
    println("\t9.1 The created key is another sample movie which does not exist originally in the dataset")
    println("\t\tWe have to set the vocab-size: " + cvModel.getVocabSize)
    println("\t\tThen our movie has actor a1 with relation acted in ai:")
    println("\t\tAnd is produced by producer p2 over relation pb")
    println("\t\tFinally is published at 20-06-17")
    println("This is how the new movie key looks like in data frame format\n")

    /*
    input:
    k: Int number of elements wanted as response for the approxNearestNeigbors answers
    key: a vector representing what we are searching for
    dataframe needed title and hashed column
     */

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
    println("This is the dataframe represenation of the key we want for our approxNearestNeighbors")
    key_movie.show()

    /* output
    +-----+--------------------+--------------------+--------------------+
    |title|             content|               words|            features|
    +-----+--------------------+--------------------+--------------------+
    |   m4|file:///Users/car...|[file:///users/ca...|(15,[4,13],[1.0,1...|
    +-----+--------------------+--------------------+--------------------+
     */

    // number of max suggestions for approxNearestNeighbors
    val k = 40

    println("Here we look for approxNearestNeighbors of our novel key")
    val key_m4 = key_movie.select("features").collect()(0)(0).asInstanceOf[Vector].toSparse
    model.approxNearestNeighbors(vectorizedDf, key_m4, k).show(false)

    /* output
    +-------------------------------------------------+-----------------------------------+---------------------------------------------------------------------------------+-------+
    |title                                            |features                           |hashValues                                                                       |distCol|
    +-------------------------------------------------+-----------------------------------+---------------------------------------------------------------------------------+-------+
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m3|(15,[2,4,9],[1.0,1.0,1.0])         |[[2.25592966E8], [4.98143035E8], [8.63894582E8], [1.247220523E9], [6.65063373E8]]|0.75   |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m1|(15,[1,11,12,13],[1.0,1.0,1.0,1.0])|[[3.54796773E8], [2.82022781E8], [3.13658409E8], [5.54649954E8], [9.2446381E7]]  |0.8    |
    +-------------------------------------------------+-----------------------------------+---------------------------------------------------------------------------------+-------+
    */

    println("And now for one existing movie m2")

    val key_m2 = vectorizedDf.select("features").collect()(1)(0).asInstanceOf[Vector].toSparse

    println("ApproxNearestNeighbors")
    model.approxNearestNeighbors(vectorizedDf, key_m2, k).show(false)

    /*
    dataframe with title and distCol which represents the results of approxNearestNeighbors
     */

    /*
    output:
    +-------------------------------------------------+---------------------------------------+---------------------------------------------------------------------------------+------------------+
    |title                                            |features                               |hashValues                                                                       |distCol           |
    +-------------------------------------------------+---------------------------------------+---------------------------------------------------------------------------------+------------------+
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m2|(15,[1,2,4,7,14],[1.0,1.0,1.0,1.0,1.0])|[[2.25592966E8], [2.82022781E8], [5.07341521E8], [8.7126731E8], [1.28502556E8]]  |0.0               |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m3|(15,[2,4,9],[1.0,1.0,1.0])             |[[2.25592966E8], [4.98143035E8], [8.63894582E8], [1.247220523E9], [6.65063373E8]]|0.6666666666666667|
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m1|(15,[1,11,12,13],[1.0,1.0,1.0,1.0])    |[[3.54796773E8], [2.82022781E8], [3.13658409E8], [5.54649954E8], [9.2446381E7]]  |0.875             |
    +-------------------------------------------------+---------------------------------------+---------------------------------------------------------------------------------+------------------+
     */

    // Self Join
    println("Approx Similarity Join")

    /*
    input:
    dataframe title hashed column
    threshold of max distance two elements are allowed to differ
    names for column for datasets, default will be datasetA and datasetB
    names for columns for titles: e.g. titleA, titleB
     */

    val threshold = 0.8
    val minhashed_df = model.approxSimilarityJoin(vectorizedDf, vectorizedDf, threshold) // .filter("distCol != 0")
    println("Whole Dataframe after minHash")
    minhashed_df.show(false)

    /*
    output
    dataframe with three columns two with the sources which are compared and third with distCol
     */

    /* output
    +-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
    |datasetA                                                                                                                                                                     |datasetB                                                                                                                                                                     |distCol           |
    +-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
    |[file:///Users/carstendraschner/GitHub/SANSA-ML/a4, (15,[0,6],[1.0,1.0]), [[7.57939931E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [7.01119548E8]]]                   |[file:///Users/carstendraschner/GitHub/SANSA-ML/a3, (15,[0],[1.0]), [[7.57939931E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [7.01119548E8]]]                         |0.5               |
    |[file:///Users/carstendraschner/GitHub/SANSA-ML/m2, (15,[1,2,4,7,14],[1.0,1.0,1.0,1.0,1.0]), [[2.25592966E8], [2.82022781E8], [5.07341521E8], [8.7126731E8], [1.28502556E8]]]|[file:///Users/carstendraschner/GitHub/SANSA-ML/m3, (15,[2,4,9],[1.0,1.0,1.0]), [[2.25592966E8], [4.98143035E8], [8.63894582E8], [1.247220523E9], [6.65063373E8]]]           |0.6666666666666667|
    |[file:///Users/carstendraschner/GitHub/SANSA-ML/m1, (15,[1,11,12,13],[1.0,1.0,1.0,1.0]), [[3.54796773E8], [2.82022781E8], [3.13658409E8], [5.54649954E8], [9.2446381E7]]]    |[file:///Users/carstendraschner/GitHub/SANSA-ML/m1, (15,[1,11,12,13],[1.0,1.0,1.0,1.0]), [[3.54796773E8], [2.82022781E8], [3.13658409E8], [5.54649954E8], [9.2446381E7]]]    |0.0               |
    |[file:///Users/carstendraschner/GitHub/SANSA-ML/a2, (15,[0,3],[1.0,1.0]), [[7.57939931E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [1.64558731E8]]]                   |[file:///Users/carstendraschner/GitHub/SANSA-ML/a4, (15,[0,6],[1.0,1.0]), [[7.57939931E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [7.01119548E8]]]                   |0.6666666666666667|
    |[file:///Users/carstendraschner/GitHub/SANSA-ML/a2, (15,[0,3],[1.0,1.0]), [[7.57939931E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [1.64558731E8]]]                   |[file:///Users/carstendraschner/GitHub/SANSA-ML/a1, (15,[3],[1.0]), [[9.78456855E8], [7.14263289E8], [4.76528358E8], [1.623173736E9], [1.64558731E8]]]                       |0.5               |
    |[file:///Users/carstendraschner/GitHub/SANSA-ML/a2, (15,[0,3],[1.0,1.0]), [[7.57939931E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [1.64558731E8]]]                   |[file:///Users/carstendraschner/GitHub/SANSA-ML/a2, (15,[0,3],[1.0,1.0]), [[7.57939931E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [1.64558731E8]]]                   |0.0               |
    |[file:///Users/carstendraschner/GitHub/SANSA-ML/m3, (15,[2,4,9],[1.0,1.0,1.0]), [[2.25592966E8], [4.98143035E8], [8.63894582E8], [1.247220523E9], [6.65063373E8]]]           |[file:///Users/carstendraschner/GitHub/SANSA-ML/m3, (15,[2,4,9],[1.0,1.0,1.0]), [[2.25592966E8], [4.98143035E8], [8.63894582E8], [1.247220523E9], [6.65063373E8]]]           |0.0               |
    |[file:///Users/carstendraschner/GitHub/SANSA-ML/m2, (15,[1,2,4,7,14],[1.0,1.0,1.0,1.0,1.0]), [[2.25592966E8], [2.82022781E8], [5.07341521E8], [8.7126731E8], [1.28502556E8]]]|[file:///Users/carstendraschner/GitHub/SANSA-ML/m2, (15,[1,2,4,7,14],[1.0,1.0,1.0,1.0,1.0]), [[2.25592966E8], [2.82022781E8], [5.07341521E8], [8.7126731E8], [1.28502556E8]]]|0.0               |
    |[file:///Users/carstendraschner/GitHub/SANSA-ML/a4, (15,[0,6],[1.0,1.0]), [[7.57939931E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [7.01119548E8]]]                   |[file:///Users/carstendraschner/GitHub/SANSA-ML/a4, (15,[0,6],[1.0,1.0]), [[7.57939931E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [7.01119548E8]]]                   |0.0               |
    |[file:///Users/carstendraschner/GitHub/SANSA-ML/a4, (15,[0,6],[1.0,1.0]), [[7.57939931E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [7.01119548E8]]]                   |[file:///Users/carstendraschner/GitHub/SANSA-ML/a2, (15,[0,3],[1.0,1.0]), [[7.57939931E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [1.64558731E8]]]                   |0.6666666666666667|
    |[file:///Users/carstendraschner/GitHub/SANSA-ML/a3, (15,[0],[1.0]), [[7.57939931E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [7.01119548E8]]]                         |[file:///Users/carstendraschner/GitHub/SANSA-ML/a4, (15,[0,6],[1.0,1.0]), [[7.57939931E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [7.01119548E8]]]                   |0.5               |
    |[file:///Users/carstendraschner/GitHub/SANSA-ML/a1, (15,[3],[1.0]), [[9.78456855E8], [7.14263289E8], [4.76528358E8], [1.623173736E9], [1.64558731E8]]]                       |[file:///Users/carstendraschner/GitHub/SANSA-ML/a2, (15,[0,3],[1.0,1.0]), [[7.57939931E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [1.64558731E8]]]                   |0.5               |
    |[file:///Users/carstendraschner/GitHub/SANSA-ML/p2, (15,[8,10],[1.0,1.0]), [[1.34279849E8], [1.89030324E8], [1.19975297E8], [1.78696741E8], [6.29007198E8]]]                 |[file:///Users/carstendraschner/GitHub/SANSA-ML/p2, (15,[8,10],[1.0,1.0]), [[1.34279849E8], [1.89030324E8], [1.19975297E8], [1.78696741E8], [6.29007198E8]]]                 |0.0               |
    |[file:///Users/carstendraschner/GitHub/SANSA-ML/m3, (15,[2,4,9],[1.0,1.0,1.0]), [[2.25592966E8], [4.98143035E8], [8.63894582E8], [1.247220523E9], [6.65063373E8]]]           |[file:///Users/carstendraschner/GitHub/SANSA-ML/m2, (15,[1,2,4,7,14],[1.0,1.0,1.0,1.0,1.0]), [[2.25592966E8], [2.82022781E8], [5.07341521E8], [8.7126731E8], [1.28502556E8]]]|0.6666666666666667|
    |[file:///Users/carstendraschner/GitHub/SANSA-ML/a3, (15,[0],[1.0]), [[7.57939931E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [7.01119548E8]]]                         |[file:///Users/carstendraschner/GitHub/SANSA-ML/a3, (15,[0],[1.0]), [[7.57939931E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [7.01119548E8]]]                         |0.0               |
    |[file:///Users/carstendraschner/GitHub/SANSA-ML/a2, (15,[0,3],[1.0,1.0]), [[7.57939931E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [1.64558731E8]]]                   |[file:///Users/carstendraschner/GitHub/SANSA-ML/a3, (15,[0],[1.0]), [[7.57939931E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [7.01119548E8]]]                         |0.5               |
    |[file:///Users/carstendraschner/GitHub/SANSA-ML/p1, (15,[5],[1.0]), [[4.4610989E8], [1.146503797E9], [1.964366928E9], [3.37005419E8], [1.165568015E9]]]                      |[file:///Users/carstendraschner/GitHub/SANSA-ML/p1, (15,[5],[1.0]), [[4.4610989E8], [1.146503797E9], [1.964366928E9], [3.37005419E8], [1.165568015E9]]]                      |0.0               |
    |[file:///Users/carstendraschner/GitHub/SANSA-ML/a3, (15,[0],[1.0]), [[7.57939931E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [7.01119548E8]]]                         |[file:///Users/carstendraschner/GitHub/SANSA-ML/a2, (15,[0,3],[1.0,1.0]), [[7.57939931E8], [6.5902527E7], [2.82845246E8], [4.95314097E8], [1.64558731E8]]]                   |0.5               |
    |[file:///Users/carstendraschner/GitHub/SANSA-ML/a1, (15,[3],[1.0]), [[9.78456855E8], [7.14263289E8], [4.76528358E8], [1.623173736E9], [1.64558731E8]]]                       |[file:///Users/carstendraschner/GitHub/SANSA-ML/a1, (15,[3],[1.0]), [[9.78456855E8], [7.14263289E8], [4.76528358E8], [1.623173736E9], [1.64558731E8]]]                       |0.0               |
    +-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
     */

    /*
    name of title column and dataset column also title for new column names
     */

    println("truncinate DataFrame after minHash")
    println("We now create two columns with clean columns for titles")
    val minhashed_output_df = minhashed_df
      .withColumn("titleA", col("datasetA").getField("title"))
      .withColumn("titleB", col("datasetB").getField("title"))
      .select("titleA", "titleB", "distCol")
    minhashed_output_df.show(false)
    println("minHash similarity Join has been Performed")

    /*
    output:
    +--------------------+--------------------+------------------+
    |            datasetA|            datasetB|           distCol|
    +--------------------+--------------------+------------------+
    |[file:///Users/ca...|[file:///Users/ca...|               0.5|
    |[file:///Users/ca...|[file:///Users/ca...|0.6666666666666667|
    |[file:///Users/ca...|[file:///Users/ca...|               0.0|
    |[file:///Users/ca...|[file:///Users/ca...|0.6666666666666667|
    |[file:///Users/ca...|[file:///Users/ca...|               0.5|
    |[file:///Users/ca...|[file:///Users/ca...|               0.0|
    |[file:///Users/ca...|[file:///Users/ca...|               0.0|
    |[file:///Users/ca...|[file:///Users/ca...|               0.0|
    |[file:///Users/ca...|[file:///Users/ca...|               0.0|
    |[file:///Users/ca...|[file:///Users/ca...|0.6666666666666667|
    |[file:///Users/ca...|[file:///Users/ca...|               0.5|
    |[file:///Users/ca...|[file:///Users/ca...|               0.5|
    |[file:///Users/ca...|[file:///Users/ca...|               0.0|
    |[file:///Users/ca...|[file:///Users/ca...|0.6666666666666667|
    |[file:///Users/ca...|[file:///Users/ca...|               0.0|
    |[file:///Users/ca...|[file:///Users/ca...|               0.5|
    |[file:///Users/ca...|[file:///Users/ca...|               0.0|
    |[file:///Users/ca...|[file:///Users/ca...|               0.5|
    |[file:///Users/ca...|[file:///Users/ca...|               0.0|
    +--------------------+--------------------+------------------+
     */

    println("10 Experiment Graph Creation:\n" +
      "\tin this part we create from our results an rdf graph which represents the gained information\n" +
      "\tWith this approach we might be able to reuse the gained information in future approaches")

    /*
    input
    resulting dataframe with needed columns for two to compare columns and one for dist col
    nameAcolumn: String e.g. titleA
    nameBcolumn: String
    Relation Names: Strings:
      elementrelation: String
      valuerelation: String
      experiment_type relation: String
      experiment_measurement_type relation: String
      experiment_datetime relation: String

    NodeName:
      experiment_name: String
      experiment_type: String
     */

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
    val experiment_measurement_type = "distance"
    val evaluation_datetime = Calendar.getInstance().getTime()
      .toString // make string out of it, in future would be better to allow date nativly in rdf
      .replaceAll("\\s", "") // remove spaces to reduce confusions with some foreign file readers
      .replaceAll(":", "") // remove also double points for less confilcting chars. todo quick and dirty hack with tostring

    val experiment_results: RDD[Triple] = minhashed_output_df
      // .withColumn("ea", col("datasetA").getField("title")) // rdd
      // .withColumn("eb", col("datasetB").getField("title"))
      // .select("ea", "eb", "distCol")
      .rdd
      .flatMap(
        row => {

          val a = row(0).toString() // .split("/").last thiese appended split and last element were needed because the initial create of rdf data leads to a uri which includes also the filepath and not the simple string
          val b = row(1).toString() // .split("/").last
          val value = row(2).toString
          val cnu: String = (experiment_type + " - " + experiment_name + " - " + a + b + evaluation_datetime).replaceAll("\\s", "")

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
            ), Triple.create(
              NodeFactory.createURI(cnu),
              NodeFactory.createURI(mtr),
              NodeFactory.createLiteral(experiment_measurement_type)
            )
          )
        }
      )
    println("Resulting Triples to represent experiment look like this (5 lines given")
    experiment_results.take(5)foreach(println(_))

    /*
    output
    file:///Users/carstendraschner/GitHub/SANSA-ML/a4 @element SematicSimilarityEstimation-Spark_Min_Hash-file:///Users/carstendraschner/GitHub/SANSA-ML/a4file:///Users/carstendraschner/GitHub/SANSA-ML/a3WedJun24140707CEST2020
    file:///Users/carstendraschner/GitHub/SANSA-ML/a3 @element SematicSimilarityEstimation-Spark_Min_Hash-file:///Users/carstendraschner/GitHub/SANSA-ML/a4file:///Users/carstendraschner/GitHub/SANSA-ML/a3WedJun24140707CEST2020
    SematicSimilarityEstimation-Spark_Min_Hash-file:///Users/carstendraschner/GitHub/SANSA-ML/a4file:///Users/carstendraschner/GitHub/SANSA-ML/a3WedJun24140707CEST2020 @value "0.5"^^http://www.w3.org/2001/XMLSchema#string
    SematicSimilarityEstimation-Spark_Min_Hash-file:///Users/carstendraschner/GitHub/SANSA-ML/a4file:///Users/carstendraschner/GitHub/SANSA-ML/a3WedJun24140707CEST2020 @experiment_type "Sematic Similarity Estimation"^^http://www.w3.org/2001/XMLSchema#string
    SematicSimilarityEstimation-Spark_Min_Hash-file:///Users/carstendraschner/GitHub/SANSA-ML/a4file:///Users/carstendraschner/GitHub/SANSA-ML/a3WedJun24140707CEST2020 @experiment_name "Spark_Min_Hash"^^http://www.w3.org/2001/XMLSchema#string
     */

    // TODO check whether this should be always public visible
    val experiment_hash: String = (experiment_type + " - " + experiment_name + " - " + evaluation_datetime).replaceAll("\\s", "")

    /*
    input:
    output path:String

     */

    val output = "/Users/carstendraschner/Downloads/experiment_results_" + experiment_hash + ".nt"
    // val output = "/Users/carstendraschner/Downloads/experiment_results"

    experiment_results.coalesce(1, shuffle = true).saveAsNTriplesFile(output)

    println("Triples are generated and stored in output path:")
    println(output)

    println("Here we start 7B")



    /*
    these are the already imp,lemented similarity estimations
    def selectSimilarity(a: Array[VertexId], b: Array[VertexId], c: Int): Double = {
        var s = 0.0
        if (c == 0) {

          /**
           * Jaccard similarity measure
           */

          val sim = intersection(a, b) / union(a, b).toDouble
          if (sim == 0.0) { s = (1 / vertex) }
          else { s = sim }

        }

        if (c == 1) {

          /**
           * Rodríguez and Egenhofer similarity measure
           */

          var g = 0.8

          val sim = (intersection(a, b) / ((g * difference(a, b)) + ((1 - g) * difference(b, a)) + intersection(a, b))).toDouble.abs
          if (sim == 0.0) { s = (1 / vertex) } // why this reassignment
          else { s = sim }

        }
        if (c == 2) {
          /**
           * The Ratio model similarity
           */
          var alph = 0.5
          var beth = 0.5

          val sim = ((intersection(a, b)) / ((alph * difference(a, b)) + (beth * difference(b, a)) + intersection(a, b))).toDouble.abs
          if (sim == 0.0) { s = (1 / vertex) }
          else { s = sim }

        }

        if (c == 3) {
          /**
           * Batet similarity measure
           */

          val cal = 1 + ((difference(a, b) + difference(b, a)) / (difference(a, b) + difference(b, a) + intersection(a, b))).abs
          val sim = log2(cal.toDouble)
          if (sim == 0.0) { s = (1 / vertex) }
          else { s = sim }
        }
        s
      }
     */
    println("We try out here alternative easy similarity estimations like jaccard on the basis of CountVectorized dataframes")
    println("Those should produce a simple DataFrame with column for uriA column for uriB and a column for the similairy results")

    println("7B.1 Jaccard")

    /*
    old title name, new tilte name a and b, column name for resulting distance column,
     */

    val jaccard = udf( (a: Vector, b: Vector) => {
      val feature_indices_a = a.toSparse.indices
      val feature_indices_b = b.toSparse.indices
      val f_set_a = feature_indices_a.toSet
      val f_set_b = feature_indices_b.toSet
      val jaccard = f_set_a.intersect(f_set_b).size.toDouble / f_set_a.union(f_set_b).size.toDouble
      jaccard
    })

    println("Create all pairs")
    val cross_for_jaccard =
      vectorizedDf
        .withColumnRenamed("title", "titleA")
        .withColumnRenamed("features", "featuresA")
        .crossJoin(
          vectorizedDf
            .withColumnRenamed("title", "titleB")
            .withColumnRenamed("features", "featuresB"))
    cross_for_jaccard.show(false)
    cross_for_jaccard.withColumn(
      "jaccardSim",
      jaccard($"featuresA", $"featuresB")
    )
      .select("titleA", "titleB", "jaccardSim")
      .show(false)

    /*
    output:
    +-------------------------------------------------+-------------------------------------------------+------------------+
    |titleA                                           |titleB                                           |jaccardSim        |
    +-------------------------------------------------+-------------------------------------------------+------------------+
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m1|file:///Users/carstendraschner/GitHub/SANSA-ML/m1|1.0               |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m1|file:///Users/carstendraschner/GitHub/SANSA-ML/m2|0.125             |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m1|file:///Users/carstendraschner/GitHub/SANSA-ML/a1|0.0               |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m1|file:///Users/carstendraschner/GitHub/SANSA-ML/m3|0.0               |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m1|file:///Users/carstendraschner/GitHub/SANSA-ML/a2|0.0               |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m1|file:///Users/carstendraschner/GitHub/SANSA-ML/a3|0.0               |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m1|file:///Users/carstendraschner/GitHub/SANSA-ML/a4|0.0               |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m1|file:///Users/carstendraschner/GitHub/SANSA-ML/p1|0.0               |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m1|file:///Users/carstendraschner/GitHub/SANSA-ML/p2|0.0               |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m2|file:///Users/carstendraschner/GitHub/SANSA-ML/m1|0.125             |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m2|file:///Users/carstendraschner/GitHub/SANSA-ML/m2|1.0               |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m2|file:///Users/carstendraschner/GitHub/SANSA-ML/a1|0.0               |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m2|file:///Users/carstendraschner/GitHub/SANSA-ML/m3|0.3333333333333333|
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m2|file:///Users/carstendraschner/GitHub/SANSA-ML/a2|0.0               |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m2|file:///Users/carstendraschner/GitHub/SANSA-ML/a3|0.0               |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m2|file:///Users/carstendraschner/GitHub/SANSA-ML/a4|0.0               |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m2|file:///Users/carstendraschner/GitHub/SANSA-ML/p1|0.0               |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/m2|file:///Users/carstendraschner/GitHub/SANSA-ML/p2|0.0               |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/a1|file:///Users/carstendraschner/GitHub/SANSA-ML/m1|0.0               |
    |file:///Users/carstendraschner/GitHub/SANSA-ML/a1|file:///Users/carstendraschner/GitHub/SANSA-ML/m2|0.0               |
    +-------------------------------------------------+-------------------------------------------------+------------------+
     */

    println("7C Rodríguez and Egenhofer")

    /*
    old title name, new title name a and b, column name for resulting distance column,
    also g
    this approach is completely reused by quick and dirty already implemented solution
     */

    val alpha: Double = 0.8
    val name_of_resulting_value_column: String = "rodriguez_egenhofer_similarity"
    val column_name_of_title: String = "title"
    val column_name_of_features: String = "features"


    val rodriguez_egenhofer = udf( (a: Vector, b: Vector, alpha: Double) => {
      val feature_indices_a = a.toSparse.indices
      val feature_indices_b = b.toSparse.indices
      val f_set_a = feature_indices_a.toSet
      val f_set_b = feature_indices_b.toSet
      val rodriguez_egenhofer = (f_set_a.intersect(f_set_b).size.toDouble) /
        ((f_set_a.intersect(f_set_b).size.toDouble) + (alpha * f_set_a.diff(f_set_b).size.toDouble) + ((1 - alpha) * f_set_b.diff(f_set_a).size.toDouble))
      rodriguez_egenhofer
    })

    println("Create all pairs")
    val cross_rodriguez_egenhofer =
      vectorizedDf
        .withColumnRenamed(column_name_of_title, "titleA")
        .withColumnRenamed(column_name_of_features, "featuresA")
        .crossJoin(
          vectorizedDf
            .withColumnRenamed(column_name_of_title, "titleB")
            .withColumnRenamed(column_name_of_features, "featuresB"))
    cross_rodriguez_egenhofer.show(false)
    cross_rodriguez_egenhofer.withColumn(
      name_of_resulting_value_column,
      rodriguez_egenhofer($"featuresA", $"featuresB", lit(alpha))
    )
      .select("titleA", "titleB", name_of_resulting_value_column)
      .show(false)

    println("7D Tversky")

    /*
    old title name, new title name a and b, column name for resulting distance column,
    also alpha and betha
    this approach is completely reused by quick and dirty already implemented solution
     */

    // val alpha: Double = 0.5
    val betha: Double = 0.5
    // val name_of_resulting_value_column: String = "tversky_similarity"
    // val column_name_of_title: String = "title"
    // val column_name_of_features: String = "features"


    val tversky = udf( (a: Vector, b: Vector, alpha: Double, betha: Double) => {
      val feature_indices_a = a.toSparse.indices
      val feature_indices_b = b.toSparse.indices
      val f_set_a = feature_indices_a.toSet
      val f_set_b = feature_indices_b.toSet
      val tversky = (f_set_a.intersect(f_set_b).size.toDouble) /
        ((f_set_a.intersect(f_set_b).size.toDouble) + (alpha * f_set_a.diff(f_set_b).size.toDouble) + (betha * f_set_b.diff(f_set_a).size.toDouble))
      tversky
    })

    println("Create all pairs")
    val cross_tversky =
      vectorizedDf
        .withColumnRenamed(column_name_of_title, "titleA")
        .withColumnRenamed(column_name_of_features, "featuresA")
        .crossJoin(
          vectorizedDf
            .withColumnRenamed(column_name_of_title, "titleB")
            .withColumnRenamed(column_name_of_features, "featuresB"))
    cross_tversky.show(false)
    cross_tversky.withColumn(
      name_of_resulting_value_column,
      tversky($"featuresA", $"featuresB", lit(alpha), lit(betha))
    )
      .select("titleA", "titleB", name_of_resulting_value_column)
      .show(false)

    //    val cal = 1 + ((difference(a, b) + difference(b, a)) / (difference(a, b) + difference(b, a) + intersection(a, b))).abs
    //          val sim = log2(cal.toDouble)

    println("7D Batet")

    /*
    old title name, new title name a and b, column name for resulting distance column,
    this approach is completely reused by quick and dirty already implemented solution
     */

    // val name_of_resulting_value_column: String = "tversky_similarity"
    // val column_name_of_title: String = "title"
    // val column_name_of_features: String = "features"


    val batet = udf( (a: Vector, b: Vector) => {
      val feature_indices_a = a.toSparse.indices
      val feature_indices_b = b.toSparse.indices
      val f_set_a = feature_indices_a.toSet
      val f_set_b = feature_indices_b.toSet
      // var log2 = (x: Double) => log10(x)/log10(2.0) TODO clear how to get log inside this udf
      // TODO clear if subsumer is allowed to be this union instead of summing up diff diff and intersect
      val tmp: Double = (1.0 + ((f_set_a.diff(f_set_b).size.toDouble + f_set_b.diff(f_set_a).size.toDouble) / f_set_a.union(f_set_b).size.toDouble))
      tmp
    })

    println("Create all pairs")
    val cross_batet =
      vectorizedDf
        .withColumnRenamed(column_name_of_title, "titleA")
        .withColumnRenamed(column_name_of_features, "featuresA")
        .crossJoin(
          vectorizedDf
            .withColumnRenamed(column_name_of_title, "titleB")
            .withColumnRenamed(column_name_of_features, "featuresB"))
    cross_batet.show(false)
    cross_batet.withColumn( // maybe this intermediate step will not be needed if we can merge log inside udf
      "tmp",
      batet($"featuresA", $"featuresB")
    ).withColumn(name_of_resulting_value_column, log2($"tmp"))
      .select("titleA", "titleB", name_of_resulting_value_column)
      .show(false)

    // easy reachable would be
    // dice, ochiai, clanquett, simpson,... all seam to be ther same lika jaqqard with slight different subsumer



    spark.stop()
  }
}
