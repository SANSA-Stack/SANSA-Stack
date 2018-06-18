## SANSA-RDF model
> The SANSA core model. Creating and manipulating RDF graphs.

This package contains most of the Triple operations used through different Spark [[[1]](#ftnt_ref1), [[2]]((#ftnt_ref2))] data structures on SANSA. 

In order to apply these transformations we first have to load the triples on one of these data representations.

### Reading RDF
- Reading RDF graph using RDD.
    ```scala
    import net.sansa_stack.rdf.spark.model.io._ 

    val lang =Lang.NTRIPLES 
    val triplesRDD = spark.rdf(lang)(path)
    ```
- Reading RDF graph using DataFrame.
    ```scala
    import net.sansa_stack.rdf.spark.model.io._ 

    val lang =Lang.NTRIPLES 
    val triplesDF = spark.read.rdf(lang)(path)
    ```

***
### RDD TripleOps
This model represents all triple manipulation based on RDD representation of the triples. It contains a set of operations on top of the RDF graph, represented as `RDD[Triple]`.
```scala
import net.sansa_stack.rdf.spark.model.rdd._ 

val triplesAsDF = triplesRDD.toDF()
```
See the [net.sansa_stack.rdf.spark.model.rdd.TripleOps](rdd/TripleOps.scala) for more function implementations.

***
### DataFrame TripleOps
This model represents all triple manipulation based on DataFrame representation of the triples. It contains a set of operations on top of the RDF graph.
```scala
import net.sansa_stack.rdf.spark.model.df._ 

val triplesAsRDD = triplesDF.toRDD()
```
See the [net.sansa_stack.rdf.spark.model.df.TripleOps](df/TripleOps.scala) for more function implementations.

***
### Dataset TripleOps
This model represents all triple manipulation based on DataSet of triples representation of the triples. It contains a set of operations on top of the RDF graph.
```scala
import net.sansa_stack.rdf.spark.model.ds._ 

val triplesAsDS = triples.toDS()
val triplesAsDF = triplesAsDS.toDF()
```
See the [net.sansa_stack.rdf.spark.model.ds.TripleOps](ds/TripleOps.scala) for more function implementations.


***
### Graph TripleOps
This model represents all triple manipulation based on GraphX representation of the triples. It contains a set of operations on top of the RDF graph.
```scala
import net.sansa_stack.rdf.spark.model.graph._ 

val triplesAsGraph = triples.asGraph()
```
See the [net.sansa_stack.rdf.spark.model.graph.GraphOps](graph/GraphOps.scala) for more function implementations.



[[1]](#ftnt_ref1)  [http://spark.apache.org/docs/latest/rdd-programming-guide.html](http://spark.apache.org/docs/latest/rdd-programming-guide.html)

[[2]](#ftnt_ref2)  [http://spark.apache.org/docs/latest/sql-programming-guide.html](http://spark.apache.org/docs/latest/sql-programming-guide.html)
