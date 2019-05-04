package net.sansa_stack.rdf.spark.model.hdt

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql
import org.scalatest.FunSuite


/*
* @author Abakar Bouba
*
* Unit Test code of net.sansa_stack.rdf.spark.model.hdt.TripleOps
*/

class TripleOpsTest extends FunSuite with DataFrameSuiteBase  {


  //val spark=new sql.SparkSession.Builder().master("local[*]").getOrCreate()
  val tripleOps=new TripleOps()
  val inputTestFile=getClass.getResource("/loader/compression-data.nt").getPath
  val result=tripleOps.readRDFFromFile(inputTestFile)
  val ROW_COUNT=7
  val DISTINCT_SUBJECT_COUNT=3
  val DISTINCT_OBJECT_COUNT=6
  val DISTINCT_PREDICATE_COUNT=5

  test("testRegisterDictionariesAsView") {
    tripleOps.registerDictionariesAsView(tripleOps.getDistinctSubjectDictDF(result),tripleOps.getDistinctObjectDictDF(result),tripleOps.getDistinctPredicateDictDF(result))
    val subjectDFCount=spark.sql(s"select * from ${TripleOps.SUBJECT_TABLE}").count()
    val objectDFCount=spark.sql(s"select * from ${TripleOps.OBJECT_TABLE}").count()
    val predicateDFCount=spark.sql(s"select * from ${TripleOps.PREDICATE_TABLE}").count()

    assert(subjectDFCount==DISTINCT_SUBJECT_COUNT)
    assert(objectDFCount==DISTINCT_OBJECT_COUNT)
    assert(predicateDFCount==DISTINCT_PREDICATE_COUNT)

  }

  test("testCreateOrReadDataSet") {
      tripleOps.createOrReadDataSet(inputTestFile,null)
      assert(tripleOps.tripleFactTable.count()==result.count())
  }

  test("testConvertRDDGraphToDF") {
    val resuleDF=tripleOps.convertRDDGraphToDF(result)
    assert(resuleDF.count()== result.count())
    assert(resuleDF.schema.fieldNames.contains("s"))
    assert(resuleDF.schema.fieldNames.contains("o"))
    assert(resuleDF.schema.fieldNames.contains("p"))
  }

  test("testGetDistinctSubjectDictDF") {
    val subjectDF=tripleOps.getDistinctSubjectDictDF(result)
    assert(subjectDF.schema.fieldNames.contains("name"))
    assert(subjectDF.schema.fieldNames.contains("index"))
    assert(subjectDF.count()==DISTINCT_SUBJECT_COUNT)
  }

  test("testGetDistinctObjectDictDF") {
    val objectDF=tripleOps.getDistinctObjectDictDF(result)
    assert(objectDF.schema.fieldNames.contains("name"))
    assert(objectDF.schema.fieldNames.contains("index"))
    assert(objectDF.count()==DISTINCT_OBJECT_COUNT)
  }

  test("testGetDistinctPredicateDictDF") {
    val predicateDF=tripleOps.getDistinctPredicateDictDF(result)
    assert(predicateDF.schema.fieldNames.contains("name"))
    assert(predicateDF.schema.fieldNames.contains("index"))
    assert(predicateDF.count()==DISTINCT_PREDICATE_COUNT)
  }
  test("testTripleSchemaSize Test") {
    assert(tripleOps.tripleSchema.fieldNames.length == 3)
  }

  test("testTripleSchema Field Name") {
    assert(tripleOps.tripleSchema.fieldNames(0).equalsIgnoreCase("s"))
    assert(tripleOps.tripleSchema.fieldNames(1).equalsIgnoreCase("o"))
    assert(tripleOps.tripleSchema.fieldNames(2).equalsIgnoreCase("p"))
  }

  test("testDictionarySchema Size Test") {
    assert(tripleOps.dictionarySchema.fieldNames.length == 2)
  }

  test("DictionarySchema Name Test") {
    assert(tripleOps.dictionarySchema.fieldNames(0).equalsIgnoreCase("name"))
    assert(tripleOps.dictionarySchema.fieldNames(1).equalsIgnoreCase("index"))
  }

  test("testReadRDFFromFile") {
      val result=tripleOps.readRDFFromFile(inputTestFile)
      assert(result.count()==ROW_COUNT)
  }



}
