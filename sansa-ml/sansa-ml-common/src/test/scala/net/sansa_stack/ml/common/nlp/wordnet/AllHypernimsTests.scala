package net.sansa_stack.ml.common.nlp.wordnet

import net.sf.extjwnl.data._
import org.scalatest.FunSuite

class AllHypernimsTests extends FunSuite {

  test("Tests getting all hypernyms of the the first synset in the word cat") {
    try {
      val wn = new WordNet
      val dict = wn.getDict
        // getting a synset by a word and index

        val cat = wn.getSynset("cat", POS.NOUN, 1).head

        val getAllHypers = wn.getAllHypernyms(cat)

         assert(getAllHypers != null)
      }
      catch {
        case e: ExceptionInInitializerError => println("The WordNet dictionary is not installed, please check the readme for instructions to enable it.")
      }

  }
}


