# SANSA Query

## Description
SANSA Query is a library to perform queries directly into [Spark](https://spark.apache.org) or [Flink](https://flink.apache.org). It allows files to reside in HDFS as well as in a local file system and distributes executions across Spark RDDs/DataFrames or Flink DataSets.

SANSA uses vertical partitioning (VP) approach and is designed to support extensible partitioning of RDF data. Instead of dealing with a single three-column table (s, p, o), data is partitioned into multiple tables based on the used RDF predicates, RDF term types and literal datatypes. The first column of these tables is always a string representing the subject. The second column always represents the literal value as a Scala/Java datatype. Tables for storing literals with language tags have an additional third string column for the language tag. Its uses [Sparqlify](https://github.com/AKSW/Sparqlify) as a scalable SPARQL-SQL rewriter.

### SANSA Query Spark
On SANSA Query Spark the method for partitioning an `RDD[Triple]` is located in [RdfPartitionUtilsSpark](https://github.com/SANSA-Stack/SANSA-RDF/blob/develop/sansa-rdf-spark-parent/sansa-rdf-spark-core/src/main/scala/net/sansa_stack/rdf/spark/partition/core/RdfPartitionUtilsSpark.scala). It uses an [RdfPartitioner](https://github.com/SANSA-Stack/SANSA-RDF/blob/develop/sansa-rdf-partition-parent/sansa-rdf-partition-core/src/main/scala/net/sansa_stack/rdf/partition/core/RdfPartitioner.scala) which maps a Triple to a single [RdfPartition](https://github.com/SANSA-Stack/SANSA-RDF/blob/develop/sansa-rdf-partition-parent/sansa-rdf-partition-core/src/main/scala/net/sansa_stack/rdf/partition/core/RdfPartition.scala) instance.

* [RdfPartition](https://github.com/SANSA-Stack/SANSA-RDF/blob/develop/sansa-rdf-partition-parent/sansa-rdf-partition-core/src/main/scala/net/sansa_stack/rdf/partition/core/RdfPartition.scala), as the name suggests, represents a partition of the RDF data and defines two methods:
  * `matches(Triple): Boolean`: This method is used to test whether a triple fits into a partition.
  * `layout: TripleLayout`: This method returns the [TripleLayout](https://github.com/SANSA-Stack/SANSA-RDF/blob/develop/sansa-rdf-partition-parent/sansa-rdf-partition-core/src/main/scala/net/sansa_stack/rdf/partition/layout/TripleLayout.scala) associated with the partition, as explained below.
  * Furthermore, RdfPartitions are expected to be serializable, and to define equals and hash code.
* TripleLayout instances are used to obtain framework-agnostic compact tabular representations of triples according to a partition. For this purpose it defines the two methods:
  * `fromTriple(triple: Triple): Product`: This method must, for a given triple, return its representation as a [Product](https://www.scala-lang.org/files/archive/api/2.11.8/index.html#scala.Product) (this is the super class of all Scala tuples)
  * `schema: Type`: This method must return the exact Scala type of the objects returned by `fromTriple`, such as `typeOf[Tuple2[String,Double]]`. Hence, layouts are expected to only yield instances of one specific type.

See the [available layouts](https://github.com/SANSA-Stack/SANSA-RDF/tree/develop/sansa-rdf-partition-parent/sansa-rdf-partition-core/src/main/scala/net/sansa_stack/rdf/partition/layout) for details.

## Usage

The following Scala code shows how to query an RDF file SPQRQL syntax (be it a local file or a file residing in HDFS):
```scala

val graphRdd = NTripleReader.load(sparkSession, new File("path/to/rdf.nt"))
 
val partitions = RdfPartitionUtilsSpark.partitionGraph(graphRdd)
val rewriter = SparqlifyUtils3.createSparqlSqlRewriter(sparkSession, partitions)
 
val qef = new QueryExecutionFactorySparqlifySpark(sparkSession, rewriter)
```
An overview is given in the [FAQ section of the SANSA project page](http://sansa-stack.net/faq/#sparql-queries). Further documentation about the builder objects can also be found on the [ScalaDoc page](http://sansa-stack.net/scaladocs/).

	

