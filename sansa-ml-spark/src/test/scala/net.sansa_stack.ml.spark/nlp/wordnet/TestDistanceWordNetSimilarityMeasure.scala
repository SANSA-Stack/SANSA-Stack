package net.sansa_stack.ml.spark.nlp.wordnet

import net.didion.jwnl.data.POS

object TestDistanceWordNetSimilarityMeasure {

  def main(args: Array[String]): Unit = {

    val wn = new WordNet

    // getting a synset by a word
    val dog = wn.synset("dog", POS.NOUN, 1).head
    val cat = wn.synset("cat", POS.NOUN, 1).head

    val wnSim = WordNetSimilarity
    Console.println(dog)

    Console.println(">>> dog.dist_similarity(cat)")
    // getting similarity of two synsets
    val dogCatPathSimilarity = wnSim.shortestPathSim(dog, cat)
    Console.println(dogCatPathSimilarity)
  }
}