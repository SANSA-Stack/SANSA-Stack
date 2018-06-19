package net.sansa_stack.ml.spark.nlp.wordnet

import net.didion.jwnl.data.POS

object TestWUPWordNetSimilarityMeasures {
  def main(args: Array[String]): Unit = {

    Console.println(">>> dog = wn.synset('dog.n.01')")
    Console.println(">>> cat = wn.synset('cat.n.01')")


    val wn = new WordNet

    val dog = wn.synset("dog", POS.NOUN, 1)
    val cat = wn.synset("cat", POS.NOUN, 1)

    val thing = wn.synset("thing", POS.NOUN, 1)

    val wnSim = WordNetSimilarity
    Console.println(dog)

    Console.println(wn.depth(dog))

    Console.println(wn.depth(thing))

    Console.println(">>> dog.wup_similarity(cat)")
    val dogCatWupSimilarity = wnSim.wupSimilarity(dog, cat)
    Console.println(dogCatWupSimilarity)


  }
}
