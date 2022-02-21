import java.lang.{ Integer => JavaInt, Long => JavaLong, String => JavaString, Double => JavaFloat }
import org.apache.ignite.IgniteCache
import org.apache.ignite.Ignition
import breeze.linalg.DenseVector
import scala.collection.mutable.Map

class Test(entityCache: Map[Int, DenseVector[Double]], relationCache: Map[Int, DenseVector[Double]], variables: Map[String, String]) {

  var dimNum = Integer.parseInt(variables("dim_num"))
  var L1_flag = Integer.parseInt(variables("L1_flag"))
  var entity_num = entityCache.size
  var hitAt = Integer.parseInt(variables("hit"))

  def sqr(x: Double): Double = {
    return x * x
  }
//
  def calc_sum(triple: TripleID): Double = {
    var sum: Double = 0
    var head = entityCache(triple.head)
    var tail = entityCache(triple.tail)
    var rel = relationCache(triple.rel)
    if (L1_flag== 1)
      for (i <- 0 until dimNum)
        sum += math.abs(tail(i) - head(i) - rel(i))
    else
      for (i <- 0 until dimNum)
        sum += sqr(tail(i) - head(i) - rel(i))

    return sum
  }

  def initMap(resultscacheMap: Map[String, Int]) {
    resultscacheMap += ("countHits4head" -> 0)
    resultscacheMap += ("countHits4headFiltered" -> 0)
    resultscacheMap += ("sumRank4head" -> 0)
    resultscacheMap += ("sumRank4headFiltered" -> 0)
    resultscacheMap += ("countHits4tail" -> 0)
    resultscacheMap += ("countHits4tailFiltered" -> 0)
    resultscacheMap += ("sumRank4tail" -> 0)
    resultscacheMap += ("sumRank4tailFiltered" -> 0)

  }

  def testSingleTriple(ids: List[Int]): Map[String, Int] = {
    val remoteIgnite = Ignition.localIgnite()
    var testCache: IgniteCache[Int, TripleID] = remoteIgnite.cache("testTriplecachee")
    var filterC: IgniteCache[TripleID, Int] = remoteIgnite.cache("allTriplesCache")
    var resultscacheMap = scala.collection.mutable.Map[String, Int]()
    initMap(resultscacheMap)
    ids.foreach(id => {
      var actualTriple = testCache.get(id)
      var h = actualTriple.head
      var t = actualTriple.tail
      var r = actualTriple.rel
      var score = calc_sum(actualTriple)

      var smallerScores4head = 0
      var filterSmallerScores4head = 0
      var smallerScores4tail = 0
      var filterSmallerScores4tail = 0
      var sumRank4head = 0
      var sumRank4headFiltered = 0
      var sumRank4tail = 0
      var sumRank4tailFiltered = 0
      var countHits4head = 0
      var countHits4tail = 0
      var countHits4headFiltered = 0
      var countHits4tailFiltered = 0
      for (i <- 0 until entity_num) {
        var sumHead = calc_sum(new TripleID(i, t, r))
        if (sumHead < score) {
          smallerScores4head += 1
          if (filterC.get(new TripleID(i, t, r)) != null) filterSmallerScores4head += 1
        }
        var sumTail = calc_sum(new TripleID(h, i, r))
        if (sumTail < score) {
          smallerScores4tail += 1
          if (filterC.get(new TripleID(h, i, r)) != null) filterSmallerScores4tail += 1
        }
      }

      if (smallerScores4head < hitAt) countHits4head = 1
      if (smallerScores4head - filterSmallerScores4head < hitAt) countHits4headFiltered = 1
      sumRank4head = smallerScores4head +1
      sumRank4headFiltered = smallerScores4head - filterSmallerScores4head +1
      resultscacheMap += ("countHits4head" -> (resultscacheMap("countHits4head") + countHits4head))
      resultscacheMap += ("countHits4headFiltered" -> (resultscacheMap("countHits4headFiltered") + countHits4headFiltered))
      resultscacheMap += ("sumRank4head" -> (resultscacheMap("sumRank4head") + sumRank4head))
      resultscacheMap += ("sumRank4headFiltered" -> (resultscacheMap("sumRank4headFiltered") + sumRank4headFiltered))

      if (smallerScores4tail < hitAt) countHits4tail = 1
      if (smallerScores4tail - filterSmallerScores4tail < hitAt) countHits4tailFiltered = 1
      sumRank4tail = smallerScores4tail +1
      sumRank4tailFiltered = smallerScores4tail - filterSmallerScores4tail +1
      resultscacheMap += ("countHits4tail" -> (resultscacheMap("countHits4tail") + countHits4tail))
      resultscacheMap += ("countHits4tailFiltered" -> (resultscacheMap("countHits4tailFiltered") + countHits4tailFiltered))
      resultscacheMap += ("sumRank4tail" -> (resultscacheMap("sumRank4tail") + sumRank4tail))
      resultscacheMap += ("sumRank4tailFiltered" -> (resultscacheMap("sumRank4tailFiltered") + sumRank4tailFiltered))
    })
//    println("first chunk done!")
    (resultscacheMap)
  }
  
  def testTriple(id: Int): Map[String, Int] = {
    val remoteIgnite = Ignition.localIgnite()
    var testCache: IgniteCache[Int, TripleID] = remoteIgnite.cache("testTriplecachee")
    var filterC: IgniteCache[TripleID, Int] = remoteIgnite.cache("allTriplesCache")
    var resultscacheMap = scala.collection.mutable.Map[String, Int]()
    initMap(resultscacheMap)

      var actualTriple = testCache.get(id)
      var h = actualTriple.head
      var t = actualTriple.tail
      var r = actualTriple.rel
      var score = calc_sum(actualTriple)

      var smallerScores4head = 0
      var filterSmallerScores4head = 0
      var smallerScores4tail = 0
      var filterSmallerScores4tail = 0
      var sumRank4head = 0
      var sumRank4headFiltered = 0
      var sumRank4tail = 0
      var sumRank4tailFiltered = 0
      var countHits4head = 0
      var countHits4tail = 0
      var countHits4headFiltered = 0
      var countHits4tailFiltered = 0
      for (i <- 0 until entity_num) {
        var sumHead = calc_sum(new TripleID(i, t, r))
        if (sumHead < score) {
          smallerScores4head += 1
          if (filterC.get(new TripleID(i, t, r)) != null) filterSmallerScores4head += 1
        }
        var sumTail = calc_sum(new TripleID(h, i, r))
        if (sumTail < score) {
          smallerScores4tail += 1
          if (filterC.get(new TripleID(h, i, r)) != null) filterSmallerScores4tail += 1
        }
      }

      if (smallerScores4head < hitAt) countHits4head = 1
      if (smallerScores4head - filterSmallerScores4head < hitAt) countHits4headFiltered = 1
      sumRank4head = smallerScores4head +1
      sumRank4headFiltered = smallerScores4head - filterSmallerScores4head +1
      resultscacheMap += ("countHits4head" -> (resultscacheMap("countHits4head") + countHits4head))
      resultscacheMap += ("countHits4headFiltered" -> (resultscacheMap("countHits4headFiltered") + countHits4headFiltered))
      resultscacheMap += ("sumRank4head" -> (resultscacheMap("sumRank4head") + sumRank4head))
      resultscacheMap += ("sumRank4headFiltered" -> (resultscacheMap("sumRank4headFiltered") + sumRank4headFiltered))

      if (smallerScores4tail < hitAt) countHits4tail = 1
      if (smallerScores4tail - filterSmallerScores4tail < hitAt) countHits4tailFiltered = 1
      sumRank4tail = smallerScores4tail +1
      sumRank4tailFiltered = smallerScores4tail - filterSmallerScores4tail +1
      resultscacheMap += ("countHits4tail" -> (resultscacheMap("countHits4tail") + countHits4tail))
      resultscacheMap += ("countHits4tailFiltered" -> (resultscacheMap("countHits4tailFiltered") + countHits4tailFiltered))
      resultscacheMap += ("sumRank4tail" -> (resultscacheMap("sumRank4tail") + sumRank4tail))
      resultscacheMap += ("sumRank4tailFiltered" -> (resultscacheMap("sumRank4tailFiltered") + sumRank4tailFiltered))

//    println("first chunk done!")
    (resultscacheMap)
  }
}