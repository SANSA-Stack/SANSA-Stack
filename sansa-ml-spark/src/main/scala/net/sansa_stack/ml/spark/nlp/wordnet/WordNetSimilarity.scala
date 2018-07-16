/*
 *
 *  WordNet for Scala and Spark
 *
 *  Afshin Sadeghi
 *  Inspired from:
 *  WordNet::Similarity of Ted Peterson
 *  and ws4j
 *  and ntlk project

 */
package net.sansa_stack.ml.spark.nlp.wordnet


object WordNetSimilarity extends WordNet {

  /**
   * Wu & Palmer (1994) method of measuring semantic relatedness based on node counting.
   * given two synsets, synset1 and synset2 returns the similarity score
   *
   * @param synset1 :Synset
   * @param synset2 :Synset
   * @return score :Double
   */
  def wupSimilarity(synset1: Synset, synset2: Synset): Double = {
    val min = 0.0
    if (synset1 == null || synset2 == null) throw new IllegalArgumentException("arg 1 or 2 was null...")

    val lcs = lowestCommonHypernym(synset1, synset2)
    if (lcs.isEmpty) return min
    val depth = this.depth(lcs.head)
    val depth1 = shortestHypernymPathLength(synset1, lcs.head) + depth
    val depth2 = shortestHypernymPathLength(synset2, lcs.head) + depth
    var score = 0.0
    if (depth1 > 0 && depth2 > 0) score = (2 * depth).toDouble / (depth1 + depth2).toDouble
    score
  }

  /**
   * Returns the distance similarity of two synsets using the shortest path linking the two synsets (if
   * one exists)
   *
   * @param synset1 : Synset
   * @param synset2 : Synset
   * @return : Double
   */
  def shortestPathSim(synset1: Synset, synset2: Synset): Double = {

    if (synset1 == null || synset2 == null) throw new IllegalArgumentException("arg 1 or 2 was null...")

    val lch = this.lowestCommonHypernym(synset1, synset2)
    val distance = lch.map(x => shortestHypernymPathLength(synset1, x) +
      shortestHypernymPathLength(synset2, x)).min
    var score = 0.0
    if (distance == 0) score = Double.PositiveInfinity
    else score = 1.toDouble / distance
    score
  }

  }
