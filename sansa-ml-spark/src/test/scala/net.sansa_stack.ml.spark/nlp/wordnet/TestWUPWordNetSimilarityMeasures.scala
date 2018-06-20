package net.sansa_stack.ml.spark.nlp.wordnet

import net.didion.jwnl.data.POS

object TestWUPWordNetSimilarityMeasures {
  def main(args: Array[String]): Unit = {

    Console.println(">>> dog = wn.synset('dog.n.01')")
    Console.println(">>> cat = wn.synset('cat.n.01')")


    val wn = new WordNet

    // getting a synset by a word
    val dog = wn.synset("dog", POS.NOUN, 1).head
    val cat = wn.synset("cat", POS.NOUN, 1).head

    val thing = wn.synset("thing", POS.NOUN, 1).head

    val wnSim = WordNetSimilarity
    Console.println(dog)

    // getting depth of the synset
    Console.println(wn.depth(dog))

    Console.println(wn.depth(thing))

    Console.println(">>> dog.wup_similarity(cat)")
    // getting similarity of two sysnsets
    val dogCatWupSimilarity = wnSim.wupSimilarity(dog, cat)
    Console.println(dogCatWupSimilarity)


  }
}
