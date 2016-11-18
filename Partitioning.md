## Partitioning of RDF data

### Utils
The [DatasetUtils](src/main/scala/net/sansa_stack/utils/spark/DatasetUtils.scala) object provides methods for
flattening (unnesting) Spark's recursive StructType instances.


### Partitioning System
The partitioning system is designed to support extensible partitioning of RDF data.


The following entities are involved:

* The method for partitioning a `RDD[Triple]` is located in  [RdfPartitionUtilsSpark](src/main/scala/net/sansa_stack/rdf/spark/partition/core/RdfPartitionUtilsSpark.scala). It uses an [RdfPartitioner](src/main/scala/net/sansa_stack/rdf/common/partition/core/RdfPartitioner.scala) which maps a Triple to a single [RdfPartition](src/main/scala/net/sansa_stack/rdf/common/partition/core/RdfPartition.scala) instance.
* `RdfPartition`, as the name suggests, represents a partition of the RDF data and defines two methods:
  * `matches(Triple): Boolean`: This method is used to test whether a triple fits into a partition.
  * `layout => TripleLayout`: This method returns the [TripleLayout](src/main/scala/net/sansa_stack/rdf/common/partition/layout/TripleLayout.scala) associated with the partition, as explained below.
  * Furthermore, `RdfPartition`s are expected to be serializable, and to define equals and hash code.
* `TripleLayout` instances are used to obtain compact tabular representations of triples according to a partition. For this purpose it defines the two methods:
  * `fromTriple(triple: Triple): Product`: This method must, for a given triple, return its representation as a `Product` (this is the super class of all scala `Tuple`s)
  * `schema: Type`: This method must return the exact scala type of the objects returned by `fromTriple`, such as `typeOf[Tuple2[String, Double]]`. Hence, layouts are expected to only yield instances of one specific type.
  * See the [available layouts](src/main/scala/net/sansa_stack/rdf/common/partition/layout) for details.



