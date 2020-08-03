val i: Integer = 1
i.toInt


// res0: Int = 937112115

/*
val spark = SparkSession.builder
  .appName(s"MinHash") // TODO where is this displayed?
  .master("local[*]") // TODO why do we need to specify this?
  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // TODO what is this for?
  .getOrCreate()
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.implicits._ // TODO does anyone know for which purposes we need

// read in data of sansa rdf
val triples: RDD[Triple] = spark.rdf(lang)(input)
val tdf: DataFrame = spark.read.rdf(lang)(input)

tdf.show(false)




val a = Array(2, 25, 32)
a.size
val b = (1 to a.size).toArray

val map1 = ((a) zip (b)).toMap
val map2 = ((b) zip (a)).toMap

val someExampleTriples = Seq(Array("u1", "r1", "l1"), Array("u1", "r2", "u2"), Array("u2", "r1", "l2"), Array("u3", "r2", "u2"))

// do this is one call
val map_uri_to_feature_list = someExampleTriples
  .flatMap(t => List((t(0), (t(1), t(2))), (t(2), (t(1), t(0))))) // map so both directions are available and store in map kind of format
  .filter(_._1.substring(0, 1) != "l") // no literals, naive by naming
  .groupBy(_._1).mapValues(_.map(_._2)) // group by starting node. so from each node perspective features are collected as list, in this special case as list of feature

// we need a node indexer in this easy example nodes are strings to map node to index and vice versa
// this is needed for lateer creation of DF

class NodeIndexer {
  private var _node_to_index_map: Map[String, Int] = _
  private var _index_to_node_map: Map[Int, String] = _
  private var _vocab_size: Int = _

  def fit(triples: Seq[Array[String]]): Unit = {
    val set_of_nodes = triples
      .flatMap(t => List(t(0), t(1), t(2))) // unfold triple
      .filter(_.substring(0, 1) != "l")

    _vocab_size = set_of_nodes.size
    val enumeration: List[Int] = (1 to _vocab_size).toList
    _node_to_index_map = (set_of_nodes zip enumeration).toMap
    _index_to_node_map = (enumeration zip set_of_nodes).toMap
  }
  def get_index(node: String): Int = _node_to_index_map(node)
  def get_node(index: Int): String = _index_to_node_map(index)
  def get_vocab_size(): Int = _vocab_size
}

val nodeIndexer = new NodeIndexer()
nodeIndexer.fit(someExampleTriples)
nodeIndexer.get_vocab_size()
nodeIndexer.get_index("u2")
nodeIndexer.get_node(3)


val arrayList = Array(map1, map2)
val someMap = arrayList.flatten.toMap
println(someMap)

val someMap1: Map[String, Seq[String]] = Map("a" -> Seq("b", "c"), "d" -> Seq("e", "f"), "e" -> Seq("e", "f"))
println(someMap1) // : Set[Seq[String]]
val someSet1: Set[Seq[String]] = someMap1.map(_._2).toSet

// this is the assignement of uri to its features
val uriToFeaturesMap: Map[String, Seq[Seq[String]]] = Map("a" -> Seq(Seq("b", "c"), Seq("e", "f")), "d" -> Seq(Seq("e", "f"), Seq("a", "c")), "e" -> Seq(Seq("e", "f")))
println(uriToFeaturesMap) // : Set[Seq[String]]

// the set of all features for each uri
val someSet2: Set[Seq[Seq[String]]] = uriToFeaturesMap.map(_._2).toSet
println(someSet2)

// this should be the list of all features
val setOfAllFeatures: Set[Seq[String]] = uriToFeaturesMap.map(_._2).flatten.toSet// someMap2.map(_._2).reduce(_ union _).distinct.collect
println(setOfAllFeatures)

val en = (1 to setOfAllFeatures.size).toArray

val someFeatureMap: Map[Seq[String], Int] = (setOfAllFeatures zip en).toMap
println(someFeatureMap)

someFeatureMap(Seq("a", "c"))

// map from uri to int
val uriIntMap: Map[String, Int] = (uriToFeaturesMap.keys zip (1 to uriToFeaturesMap.keys.size)).toMap

/* this method should map the sequence of features of
 one uris where ich sub feature is a sequence of one or
 two string to a corresponding int */
def seq_to_features(in: Seq[Seq[String]], mappingSeqHighToInt: Map[Seq[String], Int]): Seq[Tuple2[Int, Double]] = {
  in.map(s => Tuple2(mappingSeqHighToInt(s), 1.0))
}

// example to change feature set to representation minhash expects
val example_features: Seq[Seq[String]] = Seq(Seq("a", "c"), Seq("b", "c"))

// call example features as parameter in function to transform representation
val transformedFeatures: Seq[Tuple2[Int, Double]] = seq_to_features(in = example_features, mappingSeqHighToInt = someFeatureMap)

// val transformedFeaturesUris: Map[Int, Seq[Tuple2[Int, Double]]] = uriToFeaturesMap.map({case (k: String, v: Seq[Seq[String]]) => (uriIntMap(k), seq_to_features(v, someFeatureMap))})
val transformedFeaturesUris: Map[Int, Seq[Tuple2[Int, Double]]] = uriToFeaturesMap.map({case (k: String, v: Seq[Seq[String]]) => (uriIntMap(k), v.map(s => Tuple2(someFeatureMap(s), 1.0)))})



val someMapN: Map[Int, Iterable[Int]] = Map(1 -> List(2, 3), 4 -> List(7, 8))
someMapN.toSeq.toDF("a", "b") */
