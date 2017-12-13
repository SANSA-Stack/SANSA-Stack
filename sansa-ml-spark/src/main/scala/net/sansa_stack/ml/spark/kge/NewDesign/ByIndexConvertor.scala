package net.sansa_stack.ml.spark.kge.linkprediction.NewDesign

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._ 


class ByIndexConvertor(
    triples : Triples,
    spark : SparkSession) extends ConvertorTrait {

  // protected
  val entities = triples.getAllDistinctEntities().withColumn("ID", monotonicallyIncreasingId ).persist()
  
  // protected
  val predicates = triples.getAllDistinctPredicates().withColumn("ID", monotonicallyIncreasingId ).persist()
  
  def getTriplesByIndex(dsTriplesInString : Dataset[RecordStringTriples]) : Dataset[RecordLongTriples] = {
   
    val colNames = dsTriplesInString.columns
    val sub = colNames(0)
    val pred = colNames(1)
    val obj = colNames(2)
    
    val result = dsTriplesInString.withColumnRenamed(sub, "Subject")
                   .withColumnRenamed(pred,"Predicate")
                   .withColumnRenamed(obj, "Object")
                   .join(entities, col("Subject") === entities("Entities"), "left_outer")
                   .drop("Subject","Entities")
                   .withColumnRenamed("ID", "SubjectsID")
                   .join(predicates, col("Predicate") === predicates("Predicates") , "left_outer")
                   .drop("Predicate","Predicates")
                   .withColumnRenamed("ID","PredicatesID")
                   .join(entities, col("Object") === entities("Entities"), "left_outer")
                   .drop("Object","Entities")
                   .withColumnRenamed("ID","ObjectsID")
                   
    return result.asInstanceOf[Dataset[RecordLongTriples]]
  }

  def getTriplesByString(dsTriplesInLong : Dataset[RecordLongTriples]) : Dataset[RecordStringTriples] = {
       
    val colNames = dsTriplesInLong.columns
    val sub = colNames(0)
    val pred = colNames(1)
    val obj = colNames(2)
    
    val result = dsTriplesInLong.withColumnRenamed(sub, "SubjectID")
                   .withColumnRenamed(pred,"PredicateID")
                   .withColumnRenamed(obj, "ObjectID")
                   .join(entities, col("SubjectID") === entities("ID"), "left_outer")
                   .drop("SubjectID","ID")
                   .withColumnRenamed("Entities", "Subjects")
                   .join(predicates, col("PredicateID") === predicates("ID") , "left_outer")
                   .drop("PredicateID","ID")
                   .withColumnRenamed("Predicates","Predicates")
                   .join(entities, col("ObjectID") === entities("ID"), "left_outer")
                   .drop("ObjectID","ID")
                   .withColumnRenamed("Entities","Objects")
      
    return result.asInstanceOf[Dataset[RecordStringTriples]]
  }
  
  def getEntitiesByIndex(dsEntitiesIndices : Dataset[Long]) : Dataset[(String,Long)] = {
    
    val colName = dsEntitiesIndices.columns(0)
    val result = dsEntitiesIndices.join(entities, dsEntitiesIndices(colName) === entities("ID"),"left_outer")
                           .select("Entities", "ID")
                           .asInstanceOf[Dataset[(String,Long)]]
                         
    return result
  }
  
  def getPredicatesByIndex(dsPredicatesIndicies : Dataset[Long]) : Dataset[(String,Long)] = {
    
		val colName = dsPredicatesIndicies.columns(0)
    val result = dsPredicatesIndicies.join(predicates, dsPredicatesIndicies(colName) === predicates("ID"),"left_outer")
                           .select("Predicates", "ID")
                           .asInstanceOf[Dataset[(String,Long)]]
                         
    return result
  }
    
}