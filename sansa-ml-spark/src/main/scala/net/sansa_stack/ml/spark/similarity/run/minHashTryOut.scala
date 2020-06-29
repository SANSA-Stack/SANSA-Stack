package net.sansa_stack.ml.spark.similarity.run

import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model._
import org.apache.jena.riot.Lang
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.graph.Triple
import org.apache.spark.ml.linalg
import org.apache.spark.sql
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg._
import org.apache.spark.sql.types._
import net.sansa_stack.ml.spark.utils.{FeatureDataframeGenerator, NodeFeatureFactory}




object minHashTryOut {
  def main(args: Array[String]): Unit = {
    run()
  }

  def run(): Unit = {


    val spark = SparkSession.builder
      .appName(s"MinHash  tryout") // TODO where is this displayed?
      .master("local[*]") // TODO why do we need to specify this?
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // TODO what is this for?
      .getOrCreate()

    import spark.implicits._

    val input = "/Users/carstendraschner/GitHub/SANSA-ML/sansa-ml-spark/src/main/resources/rdf.nt"

    val example_triple = Triple.create(
      NodeFactory.createURI("exampleSubject"),
      NodeFactory.createURI("examplePredicate"),
      NodeFactory.createURI("exampleObject")
    )

    println(example_triple)

    val sometest: List[String] = List(example_triple.getObject().toString(), example_triple.getSubject().toString()) // List("A","B", "C")//example_triple.getObject, example_triple.getSubject)
    println(sometest)
    val somestring: String = example_triple.getObject().toString() + example_triple.getSubject().toString()
    println(somestring)

    // These are the sample triple we work with in the first place
    val tmp_array: Array[Triple] = Array(
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
    val tmp_triples: RDD[Triple] = spark.sparkContext.parallelize(tmp_array)
    println("this is the triples object wie work with")
    tmp_triples.foreach(println(_))

    println("here we produce a dense transformation into a pseudo tokenized format")
    val pseudoSeq = tmp_triples
      .flatMap(t => Seq((t.getSubject, t.getPredicate.toString() + t.getObject.toString()), (t.getObject, t.getPredicate.toString() + t.getSubject.toString())))
      .filter(_._1.isURI)
      .map({case (k, v) => (k.toString(), v)})
      .mapValues(_.replaceAll("\\s", ""))
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .map({case (k, v) => (k, v.reduceLeft(_ + " " + _))})
      .collect()
      .toSeq

    pseudoSeq.foreach(println(_))


    val pseudo_text_df = spark.createDataFrame(pseudoSeq).toDF(colNames = "title", "content")

    println("transformation done")

    pseudo_text_df.show()

    // Tokenize the wiki content
    val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words")
    val wordsDf = tokenizer.transform(pseudo_text_df)
    wordsDf.show()

    /*
    // Word count to vector for each wiki content
    val vocabSize = 1000000
    val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol("words").setOutputCol("features").setVocabSize(vocabSize).setMinDF(10).fit(wordsDf)
    val isNoneZeroVector = udf({v: Vector => v.numNonzeros > 0}, DataTypes.BooleanType)
    val vectorizedDf = cvModel
      .transform(wordsDf)
      .filter(isNoneZeroVector(col("features")))
      .select(col("title"), col("features"))
    vectorizedDf.show()
    */











    println("1")
    val fdg = new FeatureDataframeGenerator
    fdg.set_columns(List("index", "features"))
    fdg.set_mode("at")
    println("2")

    fdg.fit(tmp_triples)
    println("3")

    val tmp_map = fdg.workaround_transform()
    val tmp_df = spark.createDataFrame(tmp_map.toSeq).toDF(colNames = fdg.get_columns()(0), fdg.get_columns()(1))
    println("4")

    tmp_df.show()

    val min_hash_model = new MinHashLSH()
      .setNumHashTables(5)
      .setInputCol("features")
      .setOutputCol("hashes")

    val ex_model = min_hash_model.fit(tmp_df)

    ex_model.approxSimilarityJoin(tmp_df, tmp_df, 0.6, "JaccardDistance")
      .select(col("datasetA.index").alias("idA"),
        col("datasetB.index").alias("idB"),
        col("JaccardDistance")).show()



    /*
    // here we transform the triples into feature maps from uri to feature
    // with mode different types of modes can be set
    val nodeFeatureFactory = new NodeFeatureFactory
    nodeFeatureFactory.set_mode("at")
    println("node factory created")
    nodeFeatureFactory.fit(tmp_triples)
    println("node factory fitted")
    val sampleFeatureMap: Map[Node, Iterable[Seq[Node]]] = nodeFeatureFactory.get_fitted_map
    // TODO val sampleFeatureMap: Map[Node, Iterable[Seq[Node]]] = nodeFeatureFactory.transform(tmp_triples)
    println("nodeFeatureFactory performed transform")

    println(sampleFeatureMap)

    println("now we transformed out triples to a feature map")
    println()

    // next we need to create a feature DF where the entries are index:Int instead of URI:Node

    // instatiate node indexer to be able to swap between node and int representation



    println("instatiate nodeindexer")
    val nodeIndexer = new NodeIndexer
    nodeIndexer.fit(tmp_triples)

    println(nodeIndexer.get_vocab_size(), nodeIndexer.get_node(4))

    // try out dense form of transformation
    println("triples for both directions")
    val tmp1: RDD[(Node, Seq[Node])] = tmp_triples.flatMap(t => List((t.getSubject, Seq(t.getPredicate, t.getObject)), (t.getObject, Seq(t.getPredicate, t.getSubject))))
    tmp1.foreach(println(_))
    println("only uris")
    val tmp2: RDD[(Node, Seq[Node])] = tmp1.filter(_._1.isURI)
    tmp2.foreach(println(_))
    // create map and merge over key node
    println("merge over node as key and return map")
    val tmp3: RDD[(Node, Iterable[Seq[Node]])] = tmp2.groupBy(_._1).mapValues(_.map(_._2))
    tmp3.foreach(println(_))
    println("collect RDD to perform actions")
    val tmp4 = tmp3.collect()
    tmp4.foreach(println(_))
    // transform to int represenation in keys
    println("use indexer to transform key from node to int")
    val tmp5 = tmp4.map(kv => (nodeIndexer.get_index(kv._1), kv._2))
    tmp5.foreach(println(_))




    println("transfer to dataframe")

    // wwe need a node indexer beacuase we cannot have a dataframe with non primitive datatypes and it is desired to have it in int indexes which reduces also size


    val tmp7 = spark.createDataFrame(tmp4).toDF("id", "vals")
    // val tmp4 = tmp3.toDF("node", "features")
    tmp7.show()
    // do this is one call

    */




    /* println("experiments start here")

    def short_string(t: Triple): String = t.getSubject.toString() + t.getObject.toString()

    println("call " + short_string(example_triple))



    val pseudotext: RDD[String] = tmp_triples.map(short_string)
    pseudotext.foreach(println(_)) */




    val triples = read_in_nt_triples(
      input = input,
      spark = spark,
      lang = Lang.NTRIPLES
    )

    println("here we transform to a DF")
    triples.toDF().show()




    // triples.map(t:Triple => t.getObject()) //.foreach(println(_))

    // create features for each uri
    /* println("All Subjects")
    val subjectsUris = triples.filterObjects(_.isURI()).getSubjects.distinct.collect() // foreach(println(_))
    val predicatesUris = triples.filterObjects(_.isURI()).getPredicates.distinct.collect()
    // only objects which are uri
    val objectsUris = triples.filterObjects(_.isURI()).getObjects.distinct.collect()

    val all_Uris: Set[Node] = subjectsUris.toSet.union(predicatesUris.toSet).union(objectsUris.toSet)

    println("all Uris")
    all_Uris.foreach(println(_))

    for ( node <- all_Uris.toList) {

    } */

    println("Here we start with our min hash transformation process")

    /* this is the data format we need for our triples
    let me explain. we get a dataframe with two columns: id and features
    - the id is in our case an integer value which represents a uri
    - the features are a sequence of tuples.
      - each tuple has as first element an integer which represents the id of a unique feature
        - the space of features do rely on the feature transformation approach. i see multiple opportunities to transfer triples to features
          - an = all neighbors, so features would be all connected nodes ignoring the relation type. this will be default
          - in = all incoming neigbors so neighbor --> uri
          - on = all outgoing neigbors so neighbor <-- uri
          - ar = all relations but only relations
          - ir = incoming relations: don'tCareUri --relationUri--> uri
          - or = outgoing relations: don'tCareUri <--relationUri-- uri
          - at = all triple structure, so combined relation and node as triple
          - it = incoming triples, same as at but just incoming connections
          - ot = outgoing triples

      - the second entry is a number for IDK maybe the count or for the later reduce step. as default I will setup 1.0
     */

    val dfA = spark.createDataFrame(Seq(
      (0, Vectors.sparse(6, Seq((0, 1.0), (1, 1.0), (2, 1.0)))),
      (1, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (4, 1.0)))),
      (2, Vectors.sparse(6, Seq((0, 1.0), (2, 1.0), (4, 1.0))))
    )).toDF("id", "features")

    /* needed steps
      these steps focus of vectorization of features for nodes in graph
      - 1. get all uris
      - 2. create Map from node with uri to int
      - 3. decide way of feature transformation
      - 4. gain all features
      - 5. map features to int
      - 6. total number of features
      - 7. for each uri collect triples for feature method
      - 8. per uriIndexInt create sequence
     */

    // 1. get all uris
    println("Start Step 1")
    def get_all_uris(all_triples: RDD[Triple], collectSubjects: Boolean = true, collectPredicates: Boolean = false, collectObjects: Boolean = true): Array[Node] = {
      // get uris from each position by calling get Subject... and ensure it is uri:
      val subjectsUris = all_triples.filterObjects(_.isURI()).getSubjects.distinct.collect() // foreach(println(_))
      val predicatesUris = all_triples.filterObjects(_.isURI()).getPredicates.distinct.collect()
      val objectsUris = all_triples.filterObjects(_.isURI()).getObjects.distinct.collect()
      // merge these as sets and cast in the end to array so it becomes iterable
      val all_uris: Set[Node] = subjectsUris.toSet.union(objectsUris.toSet) // .union(predicatesUris.toSet)
      all_uris.toArray
    }
    val all_uris: Array[Node] = get_all_uris(triples)

    all_uris.foreach(println(_))
    println("We got " + all_uris.size + " different uris")
    println("Proceeded Step 1")

    // 2. ensure that number of uris fits complexity of integer. TODO is this a real problem?
    println("Start Step 2")
    def get_all_index_map(all_uris: Array[Node]): Map[Node, Int] = {
      val tmp1: Array[Node] = all_uris
      val tmp2: Array[Int] = (1 to all_uris.size).toArray
      val tmp3: Map[Node, Int] = (tmp1 zip tmp2).toMap
      tmp3
    }
    val uri_index_map: Map[Node, Int] = get_all_index_map(all_uris)
    println("map from uri to index")
    println(uri_index_map)
    println("Proceeded Step 2")

    // 3. decide for mode
    println("Start Step 3")
    val feature_creation_mode = "an"

    def triples_with_uri(node: Node, all_triples: RDD[Triple]): RDD[Triple] = {
      def containsUri(triple: Triple, node: Node): Boolean = {
        (triple.getSubject == node) || (triple.getPredicate == node) || (triple.getObject == node)
      }
      triples.filter(containsUri(_, node))
    }

    // tryout this step to get one node and to filter triples to a subset where only triples are mentioned that include this uri
    val someUri: Node = triples.filter(_.getSubject.isURI()).take(10)(8).getSubject()
    println("Selected Uri is:" + someUri)
    val someSubsetOfSomeUri: RDD[Triple] = triples_with_uri(node = someUri, all_triples = triples)
    println("Resulting Triples Subset is")
    someSubsetOfSomeUri.foreach(println(_))

    // now define a function that creates feature sequence from uri
    // differntialte between different modes
    def create_feature_sequence(uri: Node, triples_subset: RDD[Triple], mode: String = "an"): Map[Node, RDD[Seq[Node]]] = {
      if (mode == "an") {
        def an_feature(triple: Triple, uri: Node): Seq[Node] = {
          if (uri == triple.getSubject) Seq(triple.getObject)
          else if (uri == triple.getObject) Seq(triple.getSubject)
          else throw new Exception("in mode an (all neighbors) the uri is whether subject nor object")
        }
        Map(Tuple2(uri: Node, triples_subset.map(an_feature(_, uri))))
      }
      else if (mode == "at") {
        def an_feature(triple: Triple, uri: Node): Seq[Node] = {
          if (uri == triple.getSubject) Seq(triple.getPredicate, triple.getObject)
          else if (uri == triple.getObject) Seq(triple.getSubject, triple.getMatchPredicate)
          else throw new Exception("in mode at (all triples) the uri is whether subject nor object")
        }
        Map(Tuple2(uri: Node, triples_subset.map(an_feature(_, uri))))
      }
      else throw new Exception("other modes are currently not supported, currently supported are: an or at")
    }

    // now test if this function call works out
    // once again get an uri
    println("Selected Uri is once again:" + someUri)
    // reused subset of uris is
    println("Resulting Triples Subset is")
    someSubsetOfSomeUri.foreach(println(_))
    // now call the function for one uri
    val someFeaturesForSomeUri = create_feature_sequence(someUri, someSubsetOfSomeUri, mode = "at")
    println("the resulting map looks like this:")
    someFeaturesForSomeUri.foreach(println(_))
    println("these are the features for some uris")
    someFeaturesForSomeUri(someUri).foreach(println(_))
    // now call the function for all uris
    println("now call for all uris")
    val someMapOfAllUrisToAllFeatures: Array[Map[Node, RDD[Seq[Node]]]] = all_uris.map(one_uri => create_feature_sequence(one_uri, triples_with_uri(node = one_uri, all_triples = triples), mode = "at"))
    val someMergedMapOfAllUrisToAllFeatures: Map[Node, RDD[Seq[Node]]] = someMapOfAllUrisToAllFeatures.flatten.toMap
    println("try out one uri from this map")
    println(someMergedMapOfAllUrisToAllFeatures(someUri))
    someMergedMapOfAllUrisToAllFeatures(someUri).foreach(println(_))

    // 4. now collect all features to later map to inexes which are needed for later later representation
    println("Start Step 4")
    val allFeaturesList: Array[Seq[Node]] = someMergedMapOfAllUrisToAllFeatures.map(_._2).reduce(_ union _).collect().toSet.toArray

    println("These are all our different features gained from all uris")
    allFeaturesList.foreach(println(_))
    println("in total we have " + allFeaturesList.size + " different features")

    println("Proceeded Step 4")

    // start step 5
    // in this step we create a map from the sequence of nodes to a integer
    // the sequence represent a feature
    println("Start step 5")
    val featureMap: Map[Seq[Node], Int] = (allFeaturesList zip (0 to allFeaturesList.size)).toMap
    println("the mapping looks like this:")
    featureMap.foreach(println(_))
    println("Proceeded Step 5")

    // 6. total number of features
    println("Start Step 6")
    val total_number_of_features: Int = featureMap.size
    println("Total Number of Features is: " + total_number_of_features)
    println("Proceeded Step 6")

    // Transform triples of uris to data with int
    // this is the temporal dataset: someMergedMapOfAllUrisToAllFeatures
    // { case (k, v) => k -> 2 * v }
    println("Start Step 7")
    someMergedMapOfAllUrisToAllFeatures.foreach(println(_))

    // val transformedFeaturesUris: Map[Int, Seq[Tuple2[Int, Double]]] = someMergedMapOfAllUrisToAllFeatures.map({case (k: Node, v: Seq[Seq[Node]]) => (uri_index_map(k), v.map(s => Tuple2(featureMap(s), 1.0)))})
    val tmp: Map[Node, RDD[Seq[Node]]] = someMergedMapOfAllUrisToAllFeatures
    def get_sparse_vector(f: RDD[Seq[Node]], m: Map[Seq[Node], Int]): linalg.Vector = {
      val num_features: Int = m.keySet.size
      val int_rdd: RDD[Int] = f.map(fe => m(fe))
      val needed_seq: Seq[Tuple2[Int, Double]] = int_rdd.map(e => (e, 1.0)).collect().toSeq
      val res: linalg.Vector = Vectors.sparse(num_features, needed_seq)
      res
    }
    val map_of_node_index_to_vector: Map[Int, linalg.Vector] = tmp.map({case (n: Node, f: RDD[Seq[Node]]) => (uri_index_map(n), get_sparse_vector(f, featureMap): linalg.Vector)})
    val map_transformed_to_seq_for_df: Seq[Tuple2[Int, linalg.Vector]] = map_of_node_index_to_vector.toSeq
    // val tmpsome: linalg.Vector = Vectors.sparse(6, Seq((0, 1.0), (1, 1.0), (2, 1.0)))

    val neededDataForMinHashSpark = spark.createDataFrame(map_transformed_to_seq_for_df).toDF("id", "features")

    println("this is the needed data for min hash spark")
    println(neededDataForMinHashSpark)

    neededDataForMinHashSpark.show()
    println("and it should look like this:")
    dfA.show()

    println("Proceeded Step 7")

    // Here we start the example code pipeline of min Hash in apache spark for similarity estimation
    println("Here we start the example code pipeline of min Hash in apache spark for similarity estimation")
    import org.apache.spark.ml.feature._
    import org.apache.spark.ml.linalg._
    import org.apache.spark.sql.types._

    // the here starting phase is for read in and vectorization
    // this is done previously by own code
    /*
    val df = spark.read.option("delimiter", "\t").csv("/user/hadoop/testdata.tsv")
    val dfUsed = df.select(col("_c1").as("title"), col("_c4").as("content")).filter(col("content") !== null)
    dfUsed.show()

    // Tokenize the wiki content
    val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words")
    val wordsDf = tokenizer.transform(dfUsed)

    // Word count to vector for each wiki content
    val vocabSize = 1000000
    val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol("words").setOutputCol("features").setVocabSize(vocabSize).setMinDF(10).fit(wordsDf)
    val isNoneZeroVector = udf({v: Vector => v.numNonzeros > 0}, DataTypes.BooleanType)
    val vectorizedDf = cvModel.transform(wordsDf).filter(isNoneZeroVector(col("features"))).select(col("title"), col("features"))
    vectorizedDf.show()
    */

    /* val vectorizedDf: sql.DataFrame = neededDataForMinHashSpark

    val mh = new MinHashLSH().setNumHashTables(3).setInputCol("features").setOutputCol("hashValues")
    val model = mh.fit(vectorizedDf)

    model.transform(vectorizedDf).show()

    // here lines are presented which work like recommendations
    // we need  to transform the code so vectorizer are callable similar to the here presented code
    //  TODO in future
    // val key = Vectors.sparse(vocabSize, Seq((cvModel.vocabulary.indexOf("united"), 1.0), (cvModel.vocabulary.indexOf("states"), 1.0)))
    // val k = 40
    // model.approxNearestNeighbors(vectorizedDf, key, k).show()

    // no we have to guess a threshold
    // currently I dont know how to set it up
    val threshold = 0.6

    model.approxSimilarityJoin(vectorizedDf, vectorizedDf, threshold).filter("distCol != 0").show() */

    spark.stop()
  }

}
