package net.sansa_stack.ml.spark.AnomalyDetection


import org.apache.jena.graph.Node
import org.apache.spark.rdd.RDD
import org.apache.jena.graph.Triple

/*
 *
 * AnomalyDetection - Anomaly detection of numerical data
 * @objList - list of numerical type.
 * @triplesType - List of rdf type's objects.
 * @return - cluster of similar subjects.
 */

class AnomalyDetection(nTriplesRDD: RDD[Triple],
                        objList: List[String],
                        triplesType: List[String],JSimThreshold:Double) extends Serializable {
  
  def run():RDD[Set[Node]] = {

    // filtering Triples with Numerical Literal
    val TriplesWithNumericLiteral = filterNumericalTriples()
    
    if(TriplesWithNumericLiteral.count()==0){
      println("No Numerical data is present")
      System.exit(1)
    }
      
    //filter rdf type having object value dbpedia and wikidata
    val rdfTypeDBwiki = rdfType()
    
    //cluster subjects on the basis of rdf type
    val jacardSimilarity = jSimilarity(TriplesWithNumericLiteral, rdfTypeDBwiki)
    
    jacardSimilarity

  }

  def filterNumericalTriples(): RDD[Triple] = nTriplesRDD.filter(x => searchedge(x.getObject.toString(), objList))

  def searchedge(x: String, y: List[String]): Boolean = {
    if (x.contains("^")) {
      val c = x.indexOf('^')
      val subject = x.substring(c + 2)
      y.contains(subject)
    } else
      false
  }
  
  def rdfType(): RDD[(Node, Iterable[Node])] = {
    
    //filter triples with predicate as rdf:type
    val triplesWithRDFType = nTriplesRDD.filter(_.getPredicate.toString() == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
    
    
    val triplesWithDBWikidata = triplesWithRDFType.filter(f => searchType(f.getObject.toString(), triplesType))
    
    /*K-V pair of (subject,object(wwikidata or dbpedia) of RDF type triple)
       example:- (India,CompactBuffer(PopulatedPlace,country,...)*/
    val subWithType = triplesWithDBWikidata.map(f => (f.getSubject, f.getObject)).groupByKey
    
    subWithType
  }

  def searchType(x: String, y: List[String]): Boolean = {
    if (y.exists(x.contains)) {
      true
    } else
      false
  }

  def jSimilarity(TriplesWithNumericLiteral: RDD[Triple], 
                  rdfTypeDBwiki: RDD[(Node, Iterable[Node])]): RDD[Set[Node]] = {
    
    /*K-V pair of subject with their properties from numerical dtiples
      e.g (United_States,CompactBuffer(populationTotal, populationDensity))*/
    val subWithPredicate = TriplesWithNumericLiteral.map(f => (f.getSubject, f.getPredicate)).groupByKey
    
    /*join opertaion with rdf types and numerical predicates
        (India,(CompactBuffer(PopulatedPlace,country,...),CompactBuffer(populationTotal)*/
    val joinOp = rdfTypeDBwiki.join(subWithPredicate)
   
    
    //cartesian product of joinOp and filtering AxA from RDD
    val filterOnjoin = joinOp.cartesian(joinOp).
                       filter(f => (!f._1._1.toString().contentEquals(f._2._1.toString())))
    
    /*compare only those subjects which have same properties
       e.g India and USA has same properties like populationToal*/
    val subsetofProp = filterOnjoin.filter(f => f._1._2._2.toSet.subsetOf(f._2._2._2.toSet) 
                                           || f._2._2._2.toSet.subsetOf(f._1._2._2.toSet))
    
    //calculate Jaccard Similarity on rdf type of subjects
    val Jsim = subsetofProp.map(f => (f._1._1, f._2._1, Similarity.sim(f._1._2._1.toSeq, f._2._2._1.toSeq)))

    val mapSubject = Jsim.map(f => (f._1, (f._2, f._3))).groupByKey
    
    //filter the RDD with Jaccard value >= threshold
    
    val Jsimfilter = mapSubject.map {
      case (s, (iter)) =>
        (s, (iter.filter { case (s, i) => i >= JSimThreshold }))
    }
    
    
    val similarSubjects = Jsimfilter.map {
                              case (s, (iter)) =>(s, (iter.map { case (p, i) => p }))
     }
     
    
    val clusterOfSubjects = similarSubjects.map( {
      case (s, (iter)) => (iter ++ Iterable[Node](s)).toSet
    }).distinct()
    
    clusterOfSubjects

  }
}

object AnomalyDetection {
  def apply(nTriplesRDD: RDD[Triple], objList: List[String], triplesType: List[String],JSimThreshold:Double) = new AnomalyDetection(nTriplesRDD, objList, triplesType,JSimThreshold)
}
