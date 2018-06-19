/*
 *
 *  WordNet for Scala and Spark
 *
 *  Afshin Sadeghi
 */
package net.sansa_stack.ml.spark.nlp.wordnet

import java.io.{FileInputStream, InputStream, Serializable}
import java.util
import java.util.List

import net.didion.jwnl.JWNL
import net.didion.jwnl.dictionary.Dictionary


object WordNetSimilarity extends WordNet {

  /**
    *  Wu & Palmer (1994) method of measuring semantic relatedness based on node counting.
    *  given two synsets, synset1 and synset1 returns the similarity score
    * @param synset1
    * @param synset2
    * @return
    */
  def wupSimilarity(synset1: Synset, synset2: Synset): Double = {
    val min = 0.0
    if (synset1 == null || synset2 == null) throw new IllegalArgumentException("arg 1 or 2 was null...")

    val lcs = lowestCommonHypernym(synset1, synset2)
    if (lcs.isEmpty) return min
    val depth = this.minDepth(lcs.head)
    val depth1 = shortestHypernymPathLength(synset1, lcs.head) + depth
    val depth2 = shortestHypernymPathLength(synset2, lcs.head) + depth
    var score = 0.0
    if (depth1 > 0 && depth2 > 0) score = (2 * depth).toDouble / (depth1 + depth2).toDouble
    score
  }


}
