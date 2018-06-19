/*
 *
 *  WordNet for Scala and Spark
 *
 *  Afshin Sadeghi
 *  Inspired from WordNet::Similarity of Ted Peterson and https://github.com/sujitpal/scalcium and ws4j and ntlk project
*/
package net.sansa_stack.ml.spark.nlp.wordnet

import java.io.{File, FileInputStream, InputStream, Serializable}

import net.didion.jwnl.JWNL
import net.didion.jwnl.data.list.{PointerTargetNode, PointerTargetNodeList}
import net.didion.jwnl.data._
import net.didion.jwnl.dictionary.Dictionary
import scala.collection.JavaConversions._
import scala.collection.breakOut
import scala.collection.mutable.ArrayBuffer


object WordNet {

  val currentDirectory = new java.io.File(".").getCanonicalPath
  val wnConfig: InputStream = new FileInputStream(currentDirectory + "/sansa-ml-spark/config/wnconfig.xml")
  JWNL.initialize(wnConfig)
  val dict = Dictionary.getInstance()
}

class WordNet extends Serializable {
  def synsets(lemma: String): List[Synset] =
    net.didion.jwnl.data.POS.getAllPOS
      .flatMap(pos => synsets(lemma, pos.asInstanceOf[POS]))(breakOut)

  def synsets(lemma: String, pos: POS): List[Synset] = {
    val iword = WordNet.dict.getIndexWord(pos, lemma)
    if (iword == null) List.empty[Synset]
    else iword.getSenses.toList
  }

  /**
    * returns a Synset given a String
    *
    * @param lemma
    * @param pos
    * @param sid
    * @return
    */
  def getSynset(lemma: String, pos: POS, sid: Int): Option[Synset] = {
    val iword = WordNet.dict.getIndexWord(pos, lemma)
    if (iword != null) Some(iword.getSense(sid))
    else None
  }

  /**
    * Returns a Synset given a String
    *
    * @param lemma
    * @param pos
    * @param sid
    * @return
    */
  def synset(lemma: String, pos: POS, sid: Int): Synset =
    getSynset(lemma, pos, sid)
      .getOrElse(throw new NoSuchElementException(s"lemma = '$lemma', pos = $pos, sid = $sid"))

  /**
    *  Returns a Synset given a String
    * @param lemma
    * @param pos
    * @return
    */
  def synset(lemma: String, pos: POS): Synset = synset(lemma, pos, 1)

  /**
    * Gets lemma name for a Synset
    *
    * @param synset
    * @return
    */
  def lemmaNames(synset: Synset): List[String] =
    synset.getWords.map(_.getLemma)(breakOut)


  def hyponyms(ss: Synset): List[Synset] = relatedSynsets(ss, PointerType.HYPONYM)

  def hypernyms(ss: Synset): List[Synset] = relatedSynsets(ss, PointerType.HYPERNYM)

  def partMeronyms(ss: Synset): List[Synset] = relatedSynsets(ss, PointerType.PART_MERONYM)

  def partHolonyms(ss: Synset): List[Synset] = relatedSynsets(ss, PointerType.PART_HOLONYM)

  def substanceMeronyms(ss: Synset): List[Synset] = relatedSynsets(ss, PointerType.SUBSTANCE_MERONYM)

  def substanceHolonyms(ss: Synset): List[Synset] = relatedSynsets(ss, PointerType.SUBSTANCE_HOLONYM)

  def memberHolonyms(ss: Synset): List[Synset] = relatedSynsets(ss, PointerType.MEMBER_HOLONYM)

  def entailments(ss: Synset): List[Synset] = relatedSynsets(ss, PointerType.ENTAILMENT)

  def entailedBy(ss: Synset): List[Synset] = relatedSynsets(ss, PointerType.ENTAILED_BY)

  /**
    * Gets related Synsets per function
    *
    * @param ss
    * @param ptr
    * @return
    */
  def relatedSynsets(ss: Synset, ptr: PointerType): List[Synset] =
    ss.getPointers(ptr).map(ptr => ptr.getTarget.asInstanceOf[Synset])(breakOut)

  /**
    * Returns list of all hypernyms of a Synset
    *
    * @param ss
    * @return
    */
  def allHypernyms(ss: Synset): List[List[Synset]] =
    PointerUtils.getInstance()
      .getHypernymTree(ss)
      .toList
      .map(ptnl => ptnl.asInstanceOf[PointerTargetNodeList]
        .map(ptn => ptn.asInstanceOf[PointerTargetNode].getSynset)
        .toList)(breakOut)

  /**
    * Returns the list of root hypernyms of a Synset
    *
    * @param ss
    * @return
    */
  def rootHypernyms(ss: Synset): List[Synset] =
    allHypernyms(ss)
      .map(hp => hp.reverse.head).distinct

  /**
    *  Get lowestCommonHypernym of two Synsets
    * @param synset1
    * @param synset2
    * @return
    */
  def lowestCommonHypernym(synset1: Synset, synset2: Synset): List[Synset] = {
    val paths1 = allHypernyms(synset1)
    val paths2 = allHypernyms(synset2)
    lch(paths1, paths2)
  }

  /**
    * Get shortestPath Length to a Hypernim
    * @param synset1
    * @param hypernym
    * @return
    */
  def shortestHypernymPathLength(synset1: Synset, hypernym: Synset): Int = {
    val paths1 = allHypernyms(synset1)
    val path = ArrayBuffer[(Synset, Int)]()

    val matchedPath = paths1.zipWithIndex.filter { case (s, i) => s.contains(hypernym) }
    if (matchedPath.isEmpty) -1 else matchedPath.map(x => x._1.indexOf(hypernym)).min
  }

  /**
    * Returns the lowest common hypernymy of two synset paths
    * @param paths1
    * @param paths2
    * @return
    */
  private[this] def lch(paths1: List[List[Synset]], paths2: List[List[Synset]]): List[Synset] = {
    val pairs = for (paths1 <- paths1; paths2 <- paths2) yield (paths1, paths2)
    val lchs = ArrayBuffer[(Synset, Int)]()
    pairs.map { case (paths1, paths2) =>
      val lSet = paths1.toSet
      val matched = paths2.zipWithIndex.filter { case (s, i) => lSet.contains(s) }
      if (matched.nonEmpty) lchs += matched.head
    }
    var result = List[Synset]()
    if (lchs.isEmpty) {

    } else result = lchs.minBy(_._2)._1 :: result
    result
  }

  /**
    * Min depth of synset
    * @param synset
    * @return
    */
  def minDepth(synset: Synset): Int = {
    val lens = allHypernyms(synset)
    if (lens.isEmpty) -1 else lens.map(_.size).min - 1
  }

  /**
    *  Returns the depth of a synset
    * @param synset
    * @return
    */
  def depth(synset: Synset): Int = {
    val lens = allHypernyms(synset)
    if (lens.isEmpty) -1 else lens.map(_.size).max - 1
  }

  /**
    * Returns the antonym of a word
    * @param word
    * @return
    */
  def antonyms(word: Word): List[Word] =
    relatedLemmas(word, PointerType.ANTONYM)

  /**
    * return related lemmas of a word
    * @param word
    * @param ptr
    * @return
    */
  def relatedLemmas(word: Word, ptr: PointerType): List[Word] =
    word.getPointers(ptr)
      .map(ptr => ptr.getTarget.asInstanceOf[Word])(breakOut)

}