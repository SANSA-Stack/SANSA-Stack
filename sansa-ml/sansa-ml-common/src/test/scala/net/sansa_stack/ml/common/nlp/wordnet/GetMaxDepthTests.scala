package net.sansa_stack.ml.common.nlp.wordnet

import net.sf.extjwnl.data._
import org.scalatest.FunSuite

class GetMaxDepthTests extends FunSuite {

  test("Test the function that gets the maximum depth of dataset graph ") {

    try {
      val wn = new WordNet
      val dict = wn.getDict

      val thing1 = wn.getSynset("thing", POS.NOUN, 1).head
      val dog = wn.getSynset("dog", POS.NOUN, 1).head


      val dogD = wn.maxDepth(dog)
      val dogD2 = wn.minDepth(dog)
      assert(dogD != 0)
    } catch {
      case e: ExceptionInInitializerError => println("The WordNet dictionary is not installed, please check the readme for instructions to enable it.")
    }
  }
}
