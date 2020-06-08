val a = Array(2, 25, 32)
a.size
val b = (1 to a.size).toArray

val map1 = ((a) zip (b)).toMap
val map2 = ((b) zip (a)).toMap

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


