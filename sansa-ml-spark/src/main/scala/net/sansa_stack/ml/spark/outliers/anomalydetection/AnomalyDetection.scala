package net.sansa_stack.ml.spark.outliers.anomalydetection

import org.apache.jena.graph.Node
import org.apache.spark.rdd.RDD
import org.apache.jena.graph.Triple
import org.apache.spark.sql.SparkSession

/*
 *
 * AnomalyDetection - Anomaly detection of numerical data
 * @objList - list of numerical type.
 * @triplesType - List of rdf type's objects.
 * @return - cluster of similar subjects.
 */

class AnomalyDetection(nTriplesRDD: RDD[Triple], objList: List[String],
                       triplesType: List[String], JSimThreshold: Double,
                       listSuperType: List[String], sparkSession: SparkSession, hypernym: String) extends Serializable {

  def run(): RDD[(Node, Set[(Node, Node, Object)])] = {

    // filtering Triples with Numerical Literal

    // get all the triples whose objects are literal 
    //these literals also contains xsd:date as well as xsd:xsd:langstring 
    val getObjectLiteral = getObjectList()

    //remove the literal which has ^^xsd:date or xsd:langstring(only considering numerical)
    val removedLangString = getObjectLiteral.filter(f => searchedge(f.getObject.toString(), objList))

    //checking still object has only numerical data only
    val triplesWithNumericLiteral = triplesWithNumericLit(removedLangString)

    //if no numerical data, return from here
    if (triplesWithNumericLiteral.count() == 0) {
      println("No Numerical data is present")
      System.exit(1)
    }

    //get triples of hypernym
    val getHypernym = getHyp()

    //filter rdf type having object value dbpedia and join with hyernym
    val rdfTypeDBwiki = rdfType(getHypernym)

    //cluster subjects on the basis of rdf type
    val jacardSimilarity = jSimilarity(triplesWithNumericLiteral, rdfTypeDBwiki, getHypernym)

    val propCluster = propClustering(triplesWithNumericLiteral)
    jacardSimilarity

  }

  def getHyp(): RDD[Triple] = nTriplesRDD.filter(f => f.getPredicate.toString().equals(hypernym))

  def getObjectList(): RDD[Triple] = nTriplesRDD.filter(f => f.getObject.isLiteral())

  def triplesWithNumericLit(objLit: RDD[Triple]): RDD[Triple] = objLit.filter(f => isNumeric(f.getObject.toString()))

  def isNumeric(x: String): Boolean =
    {
      if (x.contains("^")) {
        val c = x.indexOf('^')
        val subject = x.substring(1, c - 1)

        if (isAllDigits(subject))
          true
        else
          false
      } else
        false
    }

  def isAllDigits(x: String): Boolean = {
    var found = false

    for (ch <- x) {
      if (ch.isDigit || ch == '.')
        found = true
      else if (ch.isLetter) {

        found = false
      }
    }

    found
  }

  //def filterNumericalTriples(): RDD[Triple] = nTriplesRDD.filter(x => searchedge(x.getObject.toString(), objList))

  def searchedge(x: String, y: List[String]): Boolean = {
    if (x.contains("^")) {
      val c = x.indexOf('^')
      val subject = x.substring(c + 2)
      !y.contains(subject)
    } else
      true
  }

  def rdfType(getHypernym: RDD[Triple]): RDD[(Node, Iterable[Iterable[String]])] = {

    //filter triples with predicate as rdf:type
    val triplesWithRDFType = nTriplesRDD.filter(_.getPredicate.toString() == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")

    val triplesWithDBWikidata = triplesWithRDFType.filter(f => searchType(f.getObject.toString(), triplesType))

    /*K-V pair of (subject,object(wwikidata or dbpedia) of RDF type triple)
       example:- (India,CompactBuffer(PopulatedPlace,country,...)*/
    val subWithType = triplesWithDBWikidata.map(f => (f.getSubject, f.getObject.toString())).groupByKey

    //adding hypernym in each subject
    val hyper = getHypernym.map(f => (f.getSubject, ("hypernym") + f.getObject)).groupByKey

    val joinOp = subWithType union hyper

    val joinOpgroup = joinOp.groupByKey

    joinOpgroup

  }

  def searchType(x: String, y: List[String]): Boolean = {
    if (y.exists(x.contains)) {
      true
    } else
      false
  }

  def jSimilarity(TriplesWithNumericLiteral: RDD[Triple],
                  rdfTypeDBwiki: RDD[(Node, Iterable[Iterable[String]])], getHypernym: RDD[Triple]): RDD[(Node, Set[(Node, Node, Object)])] = {

    /*K-V pair of subject with their properties from numerical tiples
      e.g (United_States,CompactBuffer(populationTotal, populationDensity))*/
    val subWithPredicate = TriplesWithNumericLiteral.map(f => (f.getSubject, f.getPredicate)).groupByKey

    /*join opertaion with rdf types and numerical predicates
        (India,(CompactBuffer(PopulatedPlace,country,...),CompactBuffer(populationTotal)*/
    val joinOp = rdfTypeDBwiki.join(subWithPredicate)

    val groupOp = joinOp.groupByKey

    val flatM = groupOp.map(f => (f._1, f._2.flatMap(f => f._1.flatMap(f => f))))

    //cartesian product of joinOp and discarding AxA from RDD
    val filterOnjoin = flatM.cartesian(flatM).
      filter(f => (!f._1._1.toString().contentEquals(f._2._1.toString())))  //0+2/2

    /*compare only those subjects which have same properties
       e.g India and USA has same properties like populationToal*/

    val subsetofProp = filterOnjoin.filter(f => (f._1._2.toSet.intersect(f._2._2.toSet)).size > 0)   

    val subsetofPropRemoveSubType = removeSupType(subsetofProp)
    //calculate Jaccard Similarity on rdf type of subjects

    val hypernymRDD = subsetofPropRemoveSubType.map({
      case ((a, iter), (b, iter1)) => ((a,
        iter.filter(p => p.contains("hypernym"))), ((b, iter1.filter(p => p.contains("hypernym")))))
    })
    val hypernymRDDfilter = hypernymRDD.filter(f => f._1._2.size > 0 && f._2._2.size > 0)

    val dbtypeRDD = subsetofPropRemoveSubType.map({
      case ((a, iter), (b, iter1)) => ((a,
        iter.filter(p => (!p.contains("hypernym")))), ((b, iter1.filter(p => (!p.contains("hypernym"))))))
    })

    val dbtypeRDDfilter = dbtypeRDD.filter(f => f._1._2.size > 0 && f._2._2.size > 0)

    val hypernymUdbtype = hypernymRDDfilter union dbtypeRDDfilter

    val Jsim = hypernymUdbtype.map(f => (f._1._1, f._2._1, Similarity.sim(f._1._2.toSet, f._2._2.toSet)))

    val mapSubject = Jsim.map(f => (f._1, (f._2, f._3))).groupByKey

    //filter the RDD with Jaccard value >= threshold

    val Jsimfilter = mapSubject.map {
      case (s, (iter)) =>
        (s, (iter.filter { case (s, i) => i >= JSimThreshold }))
    }

    val similarSubjects = Jsimfilter.map({
      case (s, (iter)) => (s, (iter.map { case (p, i) => p }))
    }).distinct()

    val cartSimSubject = similarSubjects.cartesian(similarSubjects).repartition(8)

    val cartSimSubjectfilter = cartSimSubject.filter(f => f._1._1 != f._2._1 && (f._2._2.toList.contains(f._1._1) &&
      f._1._2.toList.contains(f._2._1))).
      map(f => (f._1._1, (f._1._2.toSet ++ f._2._2.toSet).toSet)).distinct().cache()
  
    val simSubjectCart = cartSimSubjectfilter.cartesian(cartSimSubjectfilter).repartition(8)
   

    
    //    val subsetMembers = ght.collect{case ((set1), (set2)) if  (set2.subsetOf(set1)) && (set1 -- set2).nonEmpty => (set2) }
    val subset1 = simSubjectCart.filter {
      case ((name1, set1), (name2, set2)) => (name1 != name2) && (set2.intersect(set1).size > 0) && (set1 -- set2).nonEmpty
    }
    

    val jf = subset1.map(f => f._2)

    val superset = cartSimSubjectfilter.subtract(jf).cache

    //some extra triple removed. 
    val removedTriples = cartSimSubjectfilter.subtractByKey(superset).cache  

    val cartremove = removedTriples.cartesian(superset).filter(f => f._1._2.intersect(f._2._2).size > 0).repartition(8).
      map(f => (f._1._1, f._2._2)).distinct() 

    val final1 = cartremove.union(superset)

    val mapSubWithTriples = propClustering(TriplesWithNumericLiteral)

    val joinSimSubTriples = final1.join(mapSubWithTriples)

    val clusterOfSubjects = joinSimSubTriples.map({
      case (s, (iter, iter1)) => ((iter).toSet, iter1)
    })

    val cartClusterSub = clusterOfSubjects.cartesian(clusterOfSubjects).repartition(8)

    val subCwithNumericLiteral = cartClusterSub.
      filter(f => f._1._1.equals(f._2._1)
        && (!f._1._2.equals(f._2._2)))
      .
      map(f => (f._1._1, (f._1._2.map(f => (f._1, f._2, f._3)))))
      .groupByKey

    val propCluster = subCwithNumericLiteral.map({
      case (a, (iter)) => (iter.flatMap(f => f.map(f => f._2)).toSet, (a, (iter)))
    })

    val propDistinct = propCluster.flatMap {
      case (a, (b, (iter))) => a.map(f => (f, (b, (iter.flatMap(f => f).toSet))))
    }

    val clusterOfProp = propDistinct.map({
      case (a, (iter, iter1)) => (a, iter1.filter(f => f._2.equals(a)))
    })

    clusterOfProp

  
  }
  def isContains(a: List[Node], b: List[Node]): Boolean = {
    if (a.forall(b.contains) || b.forall(a.contains)) {
      true
    } else
      false
  }

  def removeSupType(a: RDD[((Node, Iterable[String]), (Node, Iterable[String]))]): RDD[((Node, Iterable[String]), (Node, Iterable[String]))] = {

    val rdd1 = a.map(f => f._1._2.toSet)
    val rdd2 = a.map(f => f._2._2.toSet)
    val intersectRDD = a.map(f => f._1._2.toSet.intersect(f._2._2.toSet))
    val countRDD = intersectRDD.filter({
      case b => b.size > 0
    })
    if (countRDD.count() > 0
      && !intersectRDD.equals(rdd1)
      && !intersectRDD.equals(rdd2)) {
      //     val c= a.map(f=>f._1._2.toSet.intersect(f._2._2.toSet)
      //                 .filter(f=>searchType(f.toString(),listSuperType)))
      // val c=a.map(f=>f._1._2.toSet).filter(f=>searchType(f.toString(),listSuperType))
      val clusterOfProp = a.map({
        case ((a, iter), (b, iter1)) => ((a, iter.filter(f => !searchType(f.toString(), listSuperType))),
          (b, iter1.filter(f => !searchType(f.toString(), listSuperType))))
      })

      //          
      println("------------------------------------------")
      clusterOfProp
    }   
    else
      a
  }
  def propClustering(triplesWithNumericLiteral: RDD[Triple]): RDD[(Node, Iterable[(Node, Node, Object)])] = {

    val subMap = triplesWithNumericLiteral.map(f => (f.getSubject,
      (f.getSubject, f.getPredicate, getNumber(f.getObject.toString())))) //make a function instead of using
      .groupByKey //getlitervalue because <http://dbpedia.org/datatype/squareKilometre>
    //is not a valid datatype,so give error   
    subMap
  }

  def getNumber(a: String): Object = {
    val c = a.indexOf('^')
    val subject = a.substring(1, c - 1)

    subject

  }
}
object AnomalyDetection {
  def apply(nTriplesRDD: RDD[Triple], objList: List[String], triplesType: List[String],
            JSimThreshold: Double, listSuperType: List[String], sparkSession: SparkSession, hypernym: String) = new AnomalyDetection(nTriplesRDD, objList, triplesType,
    JSimThreshold, listSuperType, sparkSession, hypernym)
}
