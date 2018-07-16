/*
 *
 *  WordNet for Scala and Spark
 *
 *  Afshin Sadeghi
 *
 *  Inspired from:
 *  WordNet::Similarity of Ted Peterson
 *  and https://github.com/sujitpal/scalcium
 *  and ws4j
 *  and nltk project
*/
package net.sansa_stack.ml.spark.nlp.wordnet

import java.io.Serializable
import scala.collection.JavaConversions._
import scala.collection.breakOut
import scala.collection.mutable.ArrayBuffer
import net.sf.extjwnl.dictionary.Dictionary
import net.sf.extjwnl.data.{PointerType, PointerUtils, Word}

/**
  * WordNet singleton to initialize WordNet dataset
  */
object WordNet {


  val dict: Dictionary = Dictionary.getDefaultResourceInstance
}

/**
  * WordNet class that provide WordNet related basic services
  */
class WordNet extends Serializable {

  /**
    * Returns an instance of the WordNet dictionary used in the package
    * @return
    */
  def getDict: Dictionary = WordNet.dict

  /**
    * Returns a Synset belonging to a lemma String
    *
    * @param lemma : String
    * @return : List[Synset]
    */
  def getSynsets(lemma: String): List[Synset] =
    net.sf.extjwnl.data.POS.getAllPOS
      .flatMap(pos => getSynsets(lemma, pos))(breakOut)


  /**
    * Returns a Synset given a String
    * Returns empty list if the lemma did not exist in the WordNet
    *
    * @param lemma : String
    * @param pos   : POS
    * @param sid   : Integer
    * @return : List[Synset]
    */
  def getSynset(lemma: String, pos: POS, sid: Int): List[Synset] = {
    val indexWord = WordNet.dict.getIndexWord(pos, lemma)
    var result = List.empty[Synset]
    if (indexWord != null) {
      result = List(indexWord.getSenses()(sid))
    }
    result
  }

  /**
    * Returns a Synset given a String, pos and synset id
    * Returns empty list if the lemma did not exist in the WordNet
    *
    * @param lemma : String
    * @param pos   : POS
    * @return List[Synset]
    */
  def getSynsets(lemma: String, pos: POS): List[Synset] = {
    val iword = WordNet.dict.getIndexWord(pos, lemma)
    if (iword == null) List.empty[Synset]
    else iword.getSenses.toList
  }

  /**
    * Gets lemma name for a synset
    *
    * @param synset :Synset
    * @return : List[String]
    */
  def lemmaNames(synset: Synset): List[String] =
    synset.getWords.map(_.getLemma)(breakOut)

  /**
    * Input is a synset
    * returns a list of synsets
    *
    * @param synset :Synset
    * @return
    */
  def hyponyms(synset: Synset): List[Synset] = relatedSynsets(synset, PointerType.HYPONYM)

  /**
    * Input is a synset
    * returns a list of synsets
    *
    * @param synset :Synset
    * @return
    */
  def hypernyms(synset: Synset): List[Synset] = relatedSynsets(synset, PointerType.HYPERNYM)

  /**
    * Input is a synset
    * returns a list of synsets
    *
    * @param synset :Synset
    * @return : List[Synset]
    */
  def partMeronyms(synset: Synset): List[Synset] = relatedSynsets(synset, PointerType.PART_MERONYM)

  /**
    * Input is a synset
    * returns a list of synsets
    *
    * @param synset :Synset
    * @return : List[Synset]
    */
  def partHolonyms(synset: Synset): List[Synset] = relatedSynsets(synset, PointerType.PART_HOLONYM)

  /**
    * Input is a synset
    * returns a list of synsets
    *
    * @param synset :Synset
    * @return : List[Synset]
    */
  def substanceMeronyms(synset: Synset): List[Synset] = relatedSynsets(synset, PointerType.SUBSTANCE_MERONYM)

  /**
    * Input is a synset
    * returns a list of synsets
    *
    * @param synset :Synset
    * @return : List[Synset]
    */
  def substanceHolonyms(synset: Synset): List[Synset] = relatedSynsets(synset, PointerType.SUBSTANCE_HOLONYM)

  /**
    * Input is a synset
    * returns a list of synsets
    *
    * @param synset :Synset
    * @return : List[Synset]
    */
  def memberHolonyms(synset: Synset): List[Synset] = relatedSynsets(synset, PointerType.MEMBER_HOLONYM)

  /**
    * Input is a synset
    * returns a list of synsets
    *
    * @param synset :Synset
    * @return : List[Synset]
    */
  def entailments(synset: Synset): List[Synset] = relatedSynsets(synset, PointerType.ENTAILMENT)

  /**
    * Gets related synsets per function given a pointer type
    * from pointer class
    *
    * @param synset :Synset
    * @param ptr    : PointerType
    * @return : List[Synset]
    */
  def relatedSynsets(synset: Synset, ptr: PointerType): List[Synset] =
    synset.getPointers(ptr).map(ptr => ptr.getTarget.asInstanceOf[Synset])(breakOut)

  /**
    * Returns list of all hypernyms of a synset
    *
    * @param synset :Synset
    * @return : List[Synset]
    */
  def allHypernyms(synset: Synset): List[List[Synset]] =
    PointerUtils
      .getHypernymTree(synset)
      .toList
      .map(ptnl => ptnl
        .map(ptn => ptn.getSynset)
        .toList)(breakOut)

  /**
    * Returns the list of root hypernyms of a Synset
    *
    * @param synset : Synset
    * @return : List[Synset]
    */
  def rootHypernyms(synset: Synset): List[Synset] =
    allHypernyms(synset)
      .map(hp => hp.reverse.head).distinct

  /**
    * Get lowestCommonHypernym of two Synsets
    *
    * @param synset1 : Synset
    * @param synset2 : Synset
    * @return : List[Synset]
    */
  def lowestCommonHypernym(synset1: Synset, synset2: Synset): List[Synset] = {
    val paths1 = allHypernyms(synset1)
    val paths2 = allHypernyms(synset2)
    lch(paths1, paths2)
  }

  /**
    * Get shortestPath Length to a Hypernim
    *
    * @param synset1  : Synset
    * @param hypernym : Synset
    * @return : Integer
    */
  def shortestHypernymPathLength(synset1: Synset, hypernym: Synset): Int = {
    val paths1 = allHypernyms(synset1)
    val path = ArrayBuffer[(Synset, Int)]()

    val matchedPath = paths1.zipWithIndex.filter { case (s, i) => s.contains(hypernym) }
    if (matchedPath.isEmpty) -1 else matchedPath.map(x => x._1.indexOf(hypernym)).min
  }

  /**
    * Returns the lowest common hypernymys of two synset paths
    *
    * @param paths1 : List[Synset]
    * @param paths2 : List[Synset]
    * @return : List[Synset]
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
    * Returns the depth of a synset
    * Since there can be several paths to root, the minimum lenth is considered
    *
    * @param synset : Synset
    * @return : Integer
    */
  def depth(synset: Synset): Int = {
    val lens = allHypernyms(synset)
    if (lens.isEmpty) -1 else lens.map(_.size).min - 1
  }

  /**
    * Returns the antonym of a word
    *
    * @param word : Word
    * @return : List[Word]
    */
  def antonyms(word: Word): List[Word] =
    relatedLemmas(word, PointerType.ANTONYM)

  /**
    * Returns related lemmas of a word given the word and the type of relation
    *
    * @param word : Word
    * @param ptr  : PointerType
    * @return : List[Word]
    */
  def relatedLemmas(word: Word, ptr: PointerType): List[Word] =
    word.getPointers(ptr)
      .map(ptr => ptr.getTarget.asInstanceOf[Word])(breakOut)

}