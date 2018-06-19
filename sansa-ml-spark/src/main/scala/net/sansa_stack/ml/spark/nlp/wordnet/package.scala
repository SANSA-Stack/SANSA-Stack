package net.sansa_stack.ml.spark.nlp

import java.io.Serializable

import net.didion.jwnl.data.POS

package object wordnet extends Serializable {
  type Synset = net.didion.jwnl.data.Synset
  type POS = net.didion.jwnl.data.POS

  val Noun = POS.NOUN
  val Verb = POS.VERB
  val Adjective = POS.ADJECTIVE
  val Adjverb = POS.ADVERB
}

