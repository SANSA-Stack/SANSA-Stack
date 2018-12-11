package net.sansa_stack.ml.common.nlp.wordnet

import net.sf.extjwnl.data._
import org.scalatest.FunSuite

class TestGetSynsets extends FunSuite {

  test("If The WordNet dictionary is correctly installed synsets must not be null ") {

    try {
      val wn = new WordNet
      val dict = wn.getDict
      // getting a synset by a word and index

      val thing1 = wn.getSynset("thing", POS.NOUN, 1).head

      // getting a list of synsets by a word
      val thing2 = wn.getSynsets("thing", POS.NOUN).head

      assert(thing1 != null)
      assert(thing2 != null)
    } catch {
      case e: ExceptionInInitializerError => println("The WordNet dictionary is not installed, please check the readme for instructions to enable it.")
    }
  }
}
