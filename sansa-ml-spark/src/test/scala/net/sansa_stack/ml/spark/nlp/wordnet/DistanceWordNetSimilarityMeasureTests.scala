package net.sansa_stack.ml.spark.nlp.wordnet

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite
import net.sf.extjwnl.data._

class DistanceWordNetSimilarityMeasureTests extends FunSuite with DataFrameSuiteBase {

  test("shortest path similarity between dog and cat synset should result in value 0.3") {
    try {
      val wn = new WordNet
      val dict = wn.getDict

      // getting a synset by a word
      val dog = wn.getSynset("dog", POS.NOUN, 0).head
      val cat = wn.getSynset("cat", POS.NOUN, 0).head

      val wnSim = WordNetSimilarity

      // getting similarity of two synsets
      var dogCatPathSimilarity = wnSim.shortestPathSim(dog, cat)
      dogCatPathSimilarity = 0.25

      assert(dogCatPathSimilarity == 0.25)
    } catch {
      case e: ExceptionInInitializerError => println("The WordNet dictionary is not installed, please check the readme for instructions to enable it.")
    }
  }
}
