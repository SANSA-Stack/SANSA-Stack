# SANSA Query
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/net.sansa-stack/sansa-query-parent_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/net.sansa-stack/sansa-query-parent_2.11)
[![Build Status](https://ci.aksw.org/jenkins/job/SANSA-Query/job/develop/badge/icon)](https://ci.aksw.org/jenkins/job/SANSA-Query/job/develop/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Twitter](https://img.shields.io/twitter/follow/SANSA_Stack.svg?style=social)](https://twitter.com/SANSA_Stack)

## Description
SANSA Query is a library to perform SPARQL queries over RDF data using big data engines [Spark](https://spark.apache.org) and [Flink](https://flink.apache.org). It allows to query RDF data that resides both in HDFS and in a local file system. Queries are executed distributed and in parallel across Spark RDDs/DataFrames or Flink DataSets. Further, SANSA-Query can query non-RDF data stored in databases e.g., MongoDB, Cassandra, MySQL or file format Parquet, using Spark.

For RDF data, SANSA uses vertical partitioning (VP) approach and is designed to support extensible partitioning of RDF data.
Instead of dealing with a single triple table (s, p, o), data is partitioned into multiple tables based on the used RDF predicates, RDF term types and literal datatypes.
The first column of these tables is always a string representing the subject.
The second column always represents the literal value as a Scala/Java datatype.
Tables for storing literals with language tags have an additional third string column for the language tag.
It supports [Sparqlify](https://github.com/AKSW/Sparqlify) or [Ontop](https://ontop-vkg.org/) as scalable SPARQL-to-SQL rewriters.

For heterogeneous data sources (data lake), SANSA uses virtual property tables (PT) partitioning, whereby data relevant to a query is loaded _on the fly_ into Spark DataFrames composed of attributes corresponding to the properties of the query. 

### SANSA Query SPARK - RDF
On SANSA Query Spark for RDF the method for partitioning an `RDD[Triple]` is located in [RdfPartitionUtilsSpark](https://github.com/SANSA-Stack/SANSA-RDF/blob/develop/sansa-rdf/sansa-rdf-spark/src/main/scala/net/sansa_stack/rdf/spark/partition/core/RdfPartitionUtilsSpark.scala). It uses an [RdfPartitioner](https://github.com/SANSA-Stack/SANSA-RDF/blob/develop/sansa-rdf/sansa-rdf-common/src/main/scala/net/sansa_stack/rdf/common/partition/core/RdfPartitioner.scala) which maps a Triple to a single [RdfPartitionStateDefault](https://github.com/SANSA-Stack/SANSA-Stack/blob/develop/sansa-rdf/sansa-rdf-common/src/main/scala/net/sansa_stack/rdf/common/partition/core/RdfPartitionStateDefault.scala) instance.

* [RdfPartition](https://github.com/SANSA-Stack/SANSA-RDF/blob/develop/sansa-rdf-common/src/main/scala/net/sansa_stack/rdf/common/partition/core/RdfPartition.scala) - as the name suggests, represents a partition of the RDF data and defines two methods:
  * `matches(Triple): Boolean`: This method is used to test whether a triple fits into a partition.
  * `layout: TripleLayout`: This method returns the [TripleLayout](https://github.com/SANSA-Stack/SANSA-RDF/blob/develop/sansa-rdf/sansa-rdf-common/src/main/scala/net/sansa_stack/rdf/common/partition/layout/TripleLayout.scala) associated with the partition, as explained below.
  * Furthermore, RdfPartitions are expected to be serializable, and to define equals and hash code.
* TripleLayout instances are used to obtain framework-agnostic compact tabular representations of triples according to a partition. For this purpose it defines the two methods:
  * `fromTriple(triple: Triple): Product`: This method must, for a given triple, return its representation as a [Product](https://www.scala-lang.org/files/archive/api/2.11.8/index.html#scala.Product) (this is the super class of all Scala tuples)
  * `schema: Type`: This method must return the exact Scala type of the objects returned by `fromTriple`, such as `typeOf[Tuple2[String, Double]]`. Hence, layouts are expected to only yield instances of one specific type.

See the [available layouts](https://github.com/SANSA-Stack/SANSA-Stack/tree/develop/sansa-rdf/sansa-rdf-common/src/main/scala/net/sansa_stack/rdf/common/partition/layout) for details.

### SANSA Query SPARK - Heterogeneous Data Sources
SANSA Query Spark for heterogeneous data sources (data data) is composed of three main components:

* [Analyser](https://github.com/SANSA-Stack/SANSA-DataLake/tree/develop/sansa-datalake-spark/src/main/scala/net/sansa_stack/datalake/spark): it extracts SPARQL triple patters and groups them by subject, it also extracts any operation on subjects like filters, group by, order by, distinct, limit.
* ِ[Planner](https://github.com/SANSA-Stack/SANSA-DataLake/blob/develop/sansa-datalake-spark/src/main/scala/net/sansa_stack/datalake/spark/Planner.scala): it extracts joins between subject-based triple patter groups and generates join plan accordingly. The join order followed is left-deep. 
* [Mapper](https://github.com/SANSA-Stack/SANSA-DataLake/blob/develop/sansa-datalake-spark/src/main/scala/net/sansa_stack/datalake/spark/Mapper.scala): it access (RML) mappings and matches properties of a subject-based triples patter group against the attributes of individual data sources. If a match exists of every property of the triple pattern, the respective data source is declared _relavant_ and loaded into Spark DataFrame. The loading into DataFrame is performed using [Spark Connectors](https://spark-packages.org/).
* [Executor](https://github.com/SANSA-Stack/SANSA-DataLake/blob/develop/sansa-datalake-spark/src/main/scala/net/sansa_stack/datalake/spark/SparkExecutor.scala): it analyses SPARQL query and generates equivalent Spark SQL functions over DataFrames, for SELECT, WHERE, GROUP-BY, ORDER-BY, LIMIT. Connection between subject-based triple pattern groups are translated into JOINs between relevant Spark DataFrames. 

## Usage

### Requirements

We currently require a Spark 2.4.x with Scala 2.12 setup.

#### Release Version
Some of our dependencies are not in Maven central, so you need to add following Maven repository to your project POM file `repositories` section:
```xml
<repository>
   <id>maven.aksw.internal</id>
   <name>AKSW Release Repository</name>
   <url>http://maven.aksw.org/archiva/repository/internal</url>
   <releases>
      <enabled>true</enabled>
   </releases>
   <snapshots>
      <enabled>false</enabled>
   </snapshots>
</repository>
```
Add the following Maven dependency to your project POM file:
```xml
<!-- SANSA Querying -->
<dependency>
   <groupId>net.sansa-stack</groupId>
   <artifactId>sansa-query-spark_2.12</artifactId>
   <version>$LATEST_RELEASE_VERSION$</version>
</dependency>
```

#### SNAPSHOT Version
While the release versions are available on Maven Central, latest SNAPSHOT versions have to be installed from source code:
```bash
git clone https://github.com/SANSA-Stack/SANSA-Stack.git
cd SANSA-Stack
mvn -am -DskipTests -pl :sansa-query-spark_2.12 clean install 
```
Alternatively, you can use the following Maven repository and add it to your project POM file `repositories` section:
```xml
<repository>
   <id>maven.aksw.snapshots</id>
   <name>AKSW Snapshot Repository</name>
   <url>http://maven.aksw.org/archiva/repository/snapshots</url>
   <releases>
      <enabled>false</enabled>
   </releases>
   <snapshots>
      <enabled>true</enabled>
   </snapshots>
</repository>
```
Then do the same as for the release version and add the dependency:
```xml
<!-- SANSA Querying -->
<dependency>
   <groupId>net.sansa-stack</groupId>
   <artifactId>sansa-query-spark_2.12</artifactId>
   <version>$LATEST_SNAPSHOT_VERSION$</version>
</dependency>
```
### Running from code
The following Scala code shows how to query an RDF file with SPARQL (be it a local file or a file residing in HDFS):

#### From file
You can find the example code also [here](https://github.com/SANSA-Stack/SANSA-Stack/blob/kryo-debug/sansa-examples/sansa-examples-spark/src/main/scala/net/sansa_stack/examples/spark/query/SPARQLExample.scala)
```scala
import net.sansa_stack.query.spark.api.domain.ResultSetSpark
import net.sansa_stack.query.spark.ontop.QueryEngineFactoryOntop
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph.Triple
import org.apache.jena.query.ResultSet
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// SparkSession is needed
val spark = SparkSession.builder
        .appName(s"SPARQL engine example")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // we need Kryo serialization enabled with some custom serializers
        .config("spark.kryo.registrator", String.join(
          ", ",
          "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
          "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator",
          "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"))
        .config("spark.sql.crossJoin.enabled", true) // needs to be enabled if your SPARQL query does make use of cartesian product Note: in Spark 3.x it's enabled by default
        .getOrCreate()

// lets assume two separate RDF files
val pathToRdfFile1 = "path/to/rdf1.nt"
val pathToRdfFile2 = "path/to/rdf2.nt"

// load the first file into an RDD of triples (from an N-Triples file here)
val triples1 = spark.rdf(Lang.NTRIPLES)(pathToRdfFile1)

// create the main query engine
// we do provide two different SPARQL-to-SQL rewriter backends, Sparqlify and Ontop
val queryEngineFactory = new QueryEngineFactoryOntop(spark) // Ontop
// or
// val queryEngineFactory = new QueryEngineFactorySparqlify(spark) // Sparqlify

// create the query execution factory for the first dataset
val qef1 = queryEngineFactory.create(triples1)

// depending on the query type, finally execute the query
doSelectQuery()
doConstructQuery()
doAskQuery()

// a) SELECT query returns a ResultSetSpark which holds an
//    RDD of bindings and the result variables
def doSelectQuery(): Unit = {
  val query = "SELECT ..."
  val qe = qef1.createQueryExecution(query)
  val result: ResultSetSpark = qe.execSelectSpark()
  val resultBindings: RDD[Binding] = result.getBindings // the bindings, i.e. mappings from vars to RDF resources
  val resultVars: Seq[Var] = result.getResultVars // the result vars of the SPARQL query
}

// b) CONSTRUCT query returns an RDD of triples
def doConstructQuery(): Unit = {
  val query = "CONSTRUCT ..."
  val qe = qef1.createQueryExecution(query)
  val result: RDD[Triple] = qe.execConstructSpark()
}

// c) ASK query returns a boolean value
def doAskQuery(): Unit = {
  val query = "ASK ..."
  val qe = qef1.createQueryExecution(query)
  val result: Boolean = qe.execAsk()
}


// you may have noticed that for SELECT and CONSTRUCT queries we used methods ending on "Spark()"
// the reason here is that those method keep the results distributed, i.e. as an RDD
// For convenience, we do also support those methods without this behaviour, i.e. the results will be fetched to the driver
// and can be processed without the Spark pros and cons:
doSelectQueryToLocal()
doConstructQueryToLocal()

// a) SELECT query returns an Apache Jena ResultSet wrapping bindings and variables
def doSelectQueryToLocal(): Unit = {
  val query = "SELECT ..."
  val qe = qef1.createQueryExecution(query)
  val result: ResultSet = qe.execSelect()
}


// b) CONSTRUCT query and return an Apache Jena Model wrapping the triples as Statements
def doConstructQueryToLocal(): Unit = {
  val query = "CONSTRUCT ..."
  val qe = qef1.createQueryExecution(query)
  val result: Model = qe.execConstruct()
}

// so far we used only a single dataset, but during the workflow you might be using different datasets
// in that case you have to take into account that a single query execution factory is immutable and bound
// to a specific datasets
// that means for another dataset we have to create another query execution factory similar to our first one:

// load the second file into an RDD of triples (from an N-Triples file here)
val triples2 = spark.rdf(Lang.NTRIPLES)(pathToRdfFile2)

// create the query execution factory for the first dataset
val qef2 = queryEngineFactory.create(triples2)

// then run queries on the second dataset by using the our new query execution factory:
val query = "SELECT ..."
val qe = qef2.createQueryExecution(query)
val result: RDD[Triple] = qe.execConstructSpark()

// if you want to run some queries on both datasets you have to merge both before creating the query execution factory
// so either do it already during loading
val triples12 = spark.rdf(Lang.NTRIPLES)(Seq(pathToRdfFile1, pathToRdfFile2).mkString(","))
// or compute the union of them
// val triples12 = triples1.union(triples2)

// then as before, create query execution factory
val qef12 = queryEngineFactory.create(triples12)

// and run the queries ...
```

### SPARQL 1.1 Language Support
With Ontop integrated and used as SPARQL to SQL rewriter we do cover the following [SPARQL 1.1](https://www.w3.org/TR/sparql11-query/) features
(unsupported features are ~~crossed out~~ )¹:

|             Section in SPARQL 1.1              |                                                               Features                                                               | Coverage |
|------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|----------|
| [5. Graph Patterns](https://www.w3.org/TR/sparql11-query/#GraphPattern)                              | `BGP`, `FILTER`                                                                                                                          | 2/2      |
| [6. Including Optional Values](https://www.w3.org/TR/sparql11-query/#optionals)                   | `OPTIONAL`                                                                                                                           | 1/1      |
| [7. Matching Alternatives](https://www.w3.org/TR/sparql11-query/#alternatives)                       | `UNION`                                                                                                                                | 1/1      |
| [8. Negation](https://www.w3.org/TR/sparql11-query/#negation)                                    | `MINUS`, ~~`FILTER [NOT] EXISTS`~~                                                                                                         | 1/2      |
| [9. Property Paths](https://www.w3.org/TR/sparql11-query/#propertypaths)                              | ~~PredicatePath~~, ~~InversePath~~, ~~ZeroOrMorePath~~, ...                                                                                      | 0        |
| [10. Assignment](https://www.w3.org/TR/sparql11-query/#assignment)                                 | `BIND`, `VALUES`                                                                                                                         | 2/2      |
| [11. Aggregates](https://www.w3.org/TR/sparql11-query/#aggregates)                                 | `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, `GROUP_CONCAT`, `SAMPLE`                                                                                      | 6/6      |
| [12. Subqueries](https://www.w3.org/TR/sparql11-query/#subqueries)                                 | Subqueries                                                                                                                           | 1/1      |
| [13. RDF Dataset](https://www.w3.org/TR/sparql11-query/#rdfDataset)                                | `GRAPH`, ~~`FROM [NAMED\]`~~                                                                                                                  | 1/2      |
| [14. Basic Federated Query](https://www.w3.org/TR/sparql11-query/#basic-federated-query)                      | ~~`SERVICE`~~                                                                                                                              | 0        |
| [15. Solution Seqs. & Mods.](https://www.w3.org/TR/sparql11-query/#solutionModifiers)                     | `ORDER BY`, `SELECT`, `DISTINCT`, `REDUCED`, `OFFSET`, `LIMIT`                                                                                  | 6/6      |
| [16. Query Forms](https://www.w3.org/TR/sparql11-query/#QueryForms)                                | `SELECT`, `CONSTRUCT`, `ASK`, `DESCRIBE`                                                                                                     | 4/4      |
| [17.4.1. Functional Forms](https://www.w3.org/TR/sparql11-query/#func-forms)                       | `BOUND`, `IF`, `COALESCE`, ~~`EXISTS`~~, ~~`NOT EXISTS`~~, `\|\|`, `&&`, `=`, `sameTerm`, ~~`IN`~~, ~~`NOT IN`~~                                                            | 7/11     |
| [17.4.2. Functions on RDF Terms](https://www.w3.org/TR/sparql11-query/#func-rdfTerms)                 | `isIRI`, `isBlank`, `isLiteral`, `isNumeric`, `str`, `lang`, `datatype`, `IRI`, `BNODE`, ~~`STRDT`~~, ~~`STRLANG`~~, `UUID`, `STRUUID`                                 | 11/13    |
| [17.4.3. Functions on Strings](https://www.w3.org/TR/sparql11-query/#func-strings)                   | `STRLEN`, `SUBSTR`, `UCASE`, `LCASE`, `STRSTARTS`, `STRENDS`, `CONTAINS`, `STRBEFORE`, `STRAFTER`, `ENCODE_FOR_URI`, `CONCAT`, `langMatches`, `REGEX`, `REPLACE` | 14/14    |
| [17.4.4. Functions on Numerics](https://www.w3.org/TR/sparql11-query/#func-numerics)                | `abs`, `round`, `ceil`, `floor`, `RAND`                                                                                                        | 5/5      |
| [17.4.5. Functions on Dates&Times](https://www.w3.org/TR/sparql11-query/#func-date-time)               | `now`, `year`, `month`, `day`, `hours`, `minutes`, `seconds`, ~~`timezone`~~, `tz`                                                                         | 8/9      |
| [17.4.6. Hash Functions](https://www.w3.org/TR/sparql11-query/#func-hash)                         | `MD5`, `SHA1`, `SHA256`, `SHA384`, `SHA512`                                                                                                    | 5/5      |
| [17.5 XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)               | ~~casting~~                                                                                                                              | 0        |
| [17.6 Extensible Value Testing](https://www.w3.org/TR/sparql11-query/#extensionFunctions)                  | ~~user defined functions~~                                                                                                               | 0        |

#### Limitations
- In the implementation of function `langMatches`, the second argument has to be a constant: allowing variables will have a negative impact on the performance in our framework.

¹ taken from the original Ontop web site at: https://ontop-vkg.org/guide/compliance.html#sparql-1-1 

### GeoSPARQL Support
We do provide preliminary GeoSPARQL support when using Ontop as SPARQL-to-SQL rewriter and Apache Sedona as
a geospatial API on top of Apache Spark.

#### XML Namespaces
The following XML namespace prefixes are used throughout this section.

| `geo:`  |     http://www.opengis.net/ont/geosparql#      |
|-------|------------------------------------------------|
| `geof:` | http://www.opengis.net/def/function/geosparql/ |

#### Usage
All you have to do is to 1) add the Apache Sedona Kryo registrator to your Spark session and 2) enable the geospatial support in the
query engine factory:

```scala
val spark = SparkSession.builder
        .appName(s"SPARQL engine example")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // we need Kryo serialization enabled with some custom serializers
        .config("spark.kryo.registrator", String.join(
          ", ",
          "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
          "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator",
          "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify",
          classOf[SedonaKryoRegistrator].getName))
        .config("spark.sql.crossJoin.enabled", true) // needs to be enabled if your SPARQL query does make use of cartesian product Note: in Spark 3.x it's enabled by default
        .getOrCreate()
val queryEngineFactory = new QueryEngineFactoryOntop(spark, enableGeospatialSuprt = true)
```



#### Features

Given that we combine two APIs to process GeoSPARQL queries, the set of supported features is obviously what
both APIs have in common. 
For Ontop the supported features are documented [here](https://ontop-vkg.org/guide/compliance.html#geosparql-1-0).

For Apache Sedona, the features are mainly documented across the [constructors](https://sedona.apache.org/api/sql/Constructor/),
[functions](https://sedona.apache.org/api/sql/Function/), and [predicates](https://sedona.apache.org/api/sql/Predicate/)
documentation.

A summary of features that should work is


|                   Section in OGC GeoSPARQL 1.0                    | Features                                                                                                                                                                  |
|-------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 8.5. WKT Serialization                                            | `geo:wktLiteral`, `geo:asWKT`                                                                                                                                             |
| 8.7. Non-Topological Query Functions                              | `geof:distance`, `geof:buffer`, `geof:convexHull` , `geof:intersection`, `geof:union`, `geof:difference`, `geof:symDifference`, `geof:envelope`, `geof:boundary`, `geof:getSRID` |
| 9.2. Common Query Functions                                       | `geof:relate`                                                                                                                                                             |
| 9.3. Topological Simple Features Relation Family Query Functions  | `geof:sfEquals`, `geof:sfDisjoint`, `geof:sfIntersects`, `geof:sfTouches`, `geof:sfCrosses`, `geof:sfWithin`, `geof:sfContains`, `geof:sfOverlaps`                                  |




#### Limitations
Currently, only WKT literals are supported, i.e. we neither support GML nor do we handle WGS84 lat/long as spatial entities.

You also cannot use the simplification provided in section "7. Topology Vocabulary Extensions - Properties" of the GeoSPARQL specs,
i.e. you cannot use the functions as predicates in triple patterns directly on geospatial feature or geometry objects.
Instead, you always have to use  the `FILTER` expression variant on the geospatial literals. For example

allowed:
```sparql
SELECT * WHERE {
   ?c1 a :City ;
      geo:hasGeometry/geo:asWKT ?geo1.
   ?c2 a :City ;
      geo:hasGeometry/geo:asWKT ?geo2.
       
     FILTER(geof:sfIntersects(?geo1, ?geo2))
}
```
not allowed:
```sparql
SELECT * WHERE {
   ?c1 a :City ;
      geo:hasGeometry ?geo1.
   ?c2 a :City ;
      geo:hasGeometry ?geo2.
   ?geo1 geo:sfIntersects ?geo2 .
}
```

Regarding optimizations, we leave it up to the user how Apache Sedona is making use its spatial index and query optimization,
so please read their [docs](https://sedona.apache.org/api/sql/Parameter/).



## Others


An overview is given in the [FAQ section of the SANSA project page](http://sansa-stack.net/faq/#sparql-queries). Further documentation about the builder objects can also be found on the [ScalaDoc page](http://sansa-stack.net/scaladocs/).

For querying heterogeneous data sources, refer to the documentation of the dedicated [SANSA-DataLake](https://github.com/SANSA-Stack/SANSA-DataLake) component.

## How to Contribute
We always welcome new contributors to the project! Please see [our contribution guide](http://sansa-stack.net/contributing-to-sansa/) for more details on how to get started contributing to SANSA.
