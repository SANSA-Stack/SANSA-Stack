---
parent: Query
title: Bindings to DataFrame
nav_order: 2
---

# Creating DataFrames from RDD[Binding]

Sansa ships with a *schema mapper* to convert SPARQL result sets to DataFrames having the appropriate datatypes.

The RDD-to-DataFrame conversion comprises the following steps:

* Configuration of a *schema mapper*
* Using the schema mapper to create a *schema mapping*
* Applying the schema mapping to an RDD in order to obtain the DataFrame


```scala
import scala.collection.JavaConverters._
import net.sansa_stack.query.spark._
import net.sansa_stack.rdf.spark.partition._

val triplesString =
"""<urn:s1> <urn:p> "2021-02-25T16:30:12Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
  |<urn:s2> <urn:p> "2021-02-26"^^<http://www.w3.org/2001/XMLSchema#date> .
  |<urn:s3> <urn:p> "5"^^<http://www.w3.org/2001/XMLSchema#int> .
  |<urn:s4> <urn:p> "6"^^<http://www.w3.org/2001/XMLSchema#long> .
  |      """.stripMargin

val it = RDFDataMgr.createIteratorTriples(IOUtils.toInputStream(triplesString, "UTF-8"), Lang.NTRIPLES, "http://example.org/").asScala.toSeq
var graphRdd: RDD[org.apache.jena.graph.Triple] = spark.sparkContext.parallelize(it)


val qef = graphRdd.verticalPartition(RdfPartitionerDefault).sparqlify

val resultSet = qef.createQueryExecution("SELECT ?o { ?s ?p ?o }")
  .execSelectSpark()

val schemaMapping = RddOfBindingToDataFrameMapper
  .configureSchemaMapper(resultSet)
  .setTypePromotionStrategy(TypePromoterImpl.create())
  .setVarToFallbackDatatype((v: Var) => null)
  .createSchemaMapping
 val df = RddOfBindingToDataFrameMapper.applySchemaMapping(resultSet.getBindings, schemaMapping)

df.show(20)

}
```

The output is a table with column names having generally the pattern `${varName}_${localNameOfDatatypeIri}`.
If for a variable there exists just a single column then the column name is the var name.

```
+----------+-------------------+------+
|    o_date|         o_datetime|o_long|
+----------+-------------------+------+
|      null|2021-02-25 17:30:12|  null|
|2021-02-26|               null|  null|
|      null|               null|     6|
|      null|               null|     5|
+----------+-------------------+------+
```

* Note that the integer and long values (5 and 6) ended up in the same column. This is because a type promoter was provided.
The type promoter gets to see *all* used datatypes and can freely remap them.
* If a datatype of a variable could not be mapped to an appropriate Spark datatype, then the callback provided by `.setVarToFallbackDatatype((v: Var) => stringOrNull)` is consulted. A fallback of `null` omits values of that type (i.e. there won't be a table column for those).
Using the fallback `xsd:string` (`(v: Var) -> XSD.xstring.getURI()`) will attempt to convert any unsupported datatype to a string value (typically via `Object::toString`).



