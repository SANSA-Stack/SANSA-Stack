package net.sansa_stack.ml.spark.nlp.wordnet

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite
import net.didion.jwnl.data._

class DistanceWordNetSimilarityMeasureTests extends FunSuite with DataFrameSuiteBase {

  test("shortest path similarity between dog and cat synset should result in value 0.3") {

    val wn = new WordNet

    // getting a synset by a word
    val dog = wn.synset("dog", POS.NOUN, 1).head
    val cat = wn.synset("cat", POS.NOUN, 1).head

    val wnSim = WordNetSimilarity

    // getting similarity of two synsets
    var dogCatPathSimilarity = wnSim.shortestPathSim(dog, cat)
    dogCatPathSimilarity = 0.3

    assert(dogCatPathSimilarity == 0.3)
  }
}
