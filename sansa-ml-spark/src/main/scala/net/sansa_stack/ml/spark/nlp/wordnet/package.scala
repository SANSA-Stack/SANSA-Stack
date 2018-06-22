package net.sansa_stack.ml.spark.nlp

import java.io.Serializable
import net.sf.extjwnl.data.POS

package object wordnet extends Serializable {

  type Synset = net.sf.extjwnl.data.Synset
  type POS = net.sf.extjwnl.data.POS

  val Noun = net.sf.extjwnl.data.POS.NOUN
  val Verb = POS.VERB
  val Adjective = POS.ADJECTIVE
  val Adjverb = POS.ADVERB
}

