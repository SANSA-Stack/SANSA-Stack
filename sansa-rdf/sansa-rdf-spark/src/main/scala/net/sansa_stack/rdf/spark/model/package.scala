package net.sansa_stack.rdf.spark

import net.sansa_stack.rdf.spark.utils.Logging
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.spark.graphx._
import org.apache.jena.graph.{Graph => JenaGraph}
import org.apache.jena.sparql.graph.GraphFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Wrap up implicit classes/methods for RDF data into [[RDD]].
  *
  * @author Gezim Sejdiu
  */

package object model {

  /**
    * Adds all methods to [[RDD]] that allows to use TripleOps functions.
    */
  implicit class TripleOperations(triples: RDD[Triple]) extends Logging {

    import net.sansa_stack.rdf.spark.rdd.op.RddOfTriplesOps

    /**
      * Convert a [[RDD[Triple]]] into a DataFrame.
      *
      * @return a DataFrame of triples.
      * @see [[RddOfTriplesOps.toDF]]
      */
    def toDF(): DataFrame =
      RddOfTriplesOps.toDF(triples)

    /**
      * Convert an RDD of Triple into a Dataset of Triple.
      *
      * @return a Dataset of triples.
      * @see [[RddOfTriplesOps.toDS]]
      */
    def toDS(): Dataset[Triple] =
      RddOfTriplesOps.toDS(triples)

    /**
      * Get subjects.
      *
      * @return [[RDD[Node]]] which contains list of the subjects.
      * @see [[RddOfTriplesOps.getSubjects]]
      */
    def getSubjects(): RDD[Node] =
      RddOfTriplesOps.getSubjects(triples)

    /**
      * Get predicates.
      *
      * @return [[RDD[Node]]] which contains list of the predicates.
      * @see [[RddOfTriplesOps.getPredicates]]
      */
    def getPredicates(): RDD[Node] =
      RddOfTriplesOps.getPredicates(triples)

    /**
      * Get objects.
      *
      * @return [[RDD[Node]]] which contains list of the objects.
      * @see [[RddOfTriplesOps.getObjects]]
      */
    def getObjects(): RDD[Node] =
      RddOfTriplesOps.getObjects(triples)

    /**
      * Filter out the subject from a given RDD[Triple],
      * based on a specific function @func .
      *
      * @param func a partial funtion.
      * @return [[RDD[Triple]]] a subset of the given RDD.
      * @see [[RddOfTriplesOps.filterSubjects]]
      */
    def filterSubjects(func: Node => Boolean): RDD[Triple] =
      RddOfTriplesOps.filterSubjects(triples, func)

    /**
      * Filter out the predicates from a given RDD[Triple],
      * based on a specific function @func .
      *
      * @param func a partial funtion.
      * @return [[RDD[Triple]]] a subset of the given RDD.
      * @see [[RddOfTriplesOps.filterPredicates]]
      */
    def filterPredicates(func: Node => Boolean): RDD[Triple] =
      RddOfTriplesOps.filterPredicates(triples, func)

    /**
      * Filter out the objects from a given RDD[Triple],
      * based on a specific function @func .
      *
      * @param func a partial funtion.
      * @return [[RDD[Triple]]] a subset of the given RDD.
      * @see [[RddOfTriplesOps.filterObjects]]
      */
    def filterObjects(func: Node => Boolean): RDD[Triple] =
      RddOfTriplesOps.filterObjects(triples, func)

    /**
      * Returns an RDD of triples that match with the given input.
      *
      * @param subject   the subject
      * @param predicate the predicate
      * @param object    the object
      * @return RDD of triples
      * @see [[RddOfTriplesOps.find]]
      */
    def find(subject: Option[Node] = None, predicate: Option[Node] = None, `object`: Option[Node] = None): RDD[Triple] =
      RddOfTriplesOps.find(triples, subject, predicate, `object`)

    /**
      * Returns an RDD of triples that match with the given input.
      *
      * @param triple the triple to be checked
      * @return RDD of triples that match the given input
      * @see [[RddOfTriplesOps.find]]
      */
    def find(triple: Triple): RDD[Triple] =
      RddOfTriplesOps.find(triples, triple)

    /**
      * Determine whether this RDF graph contains any triples
      * with a given subject and predicate.
      *
      * @param subject   the subject (None for any)
      * @param predicate the predicate (Node for any)
      * @return true if there exists within this RDF graph
      *         a triple with subject and predicate, false otherwise
      */
    def contains(subject: Some[Node], predicate: Some[Node]): Boolean =
      RddOfTriplesOps.contains(triples, subject, predicate, None)

    /**
      * Determine whether this RDF graph contains any triples
      * with a given (subject, predicate, object) pattern.
      *
      * @param subject   the subject (None for any)
      * @param predicate the predicate (None for any)
      * @param object    the object (None for any)
      * @return true if there exists within this RDF graph
      *         a triple with (S, P, O) pattern, false otherwise
      */
    def contains(subject: Some[Node], predicate: Some[Node], `object`: Some[Node]): Boolean =
      RddOfTriplesOps.contains(triples, subject, predicate, `object`)

    /**
      * Determine if a triple is present in this RDF graph.
      *
      * @param triple the triple to be checked
      * @return true if the statement s is in this RDF graph, false otherwise
      */
    def contains(triple: Triple): Boolean =
      RddOfTriplesOps.contains(triples, triple)

    /**
      * Determine if any of the triples in an RDF graph are also contained in this RDF graph.
      *
      * @param other the other RDF graph containing the statements to be tested
      * @return true if any of the statements in RDF graph are also contained
      *         in this RDF graph and false otherwise.
      */
    def containsAny(other: RDD[Triple]): Boolean =
      RddOfTriplesOps.containsAny(triples, other)

    /**
      * Determine if all of the statements in an RDF graph are also contained in this RDF graph.
      *
      * @param other the other RDF graph containing the statements to be tested
      * @return true if all of the statements in RDF graph are also contained
      *         in this RDF graph and false otherwise.
      */
    def containsAll(other: RDD[Triple]): Boolean =
      RddOfTriplesOps.containsAll(triples, other)

    /**
      * Return the union all of RDF graphs.
      *
      * @return graph (union of all)
      * @see [[RddOfTriplesOps.unionAll]]
      */
    def unionAll(others: Seq[RDD[Triple]]): RDD[Triple] =
      RddOfTriplesOps.unionAll(triples, others)

    /**
      * Add a statement to the current RDF graph.
      *
      * @param triple the triple to be added.
      * @return new RDD of triples containing this statement.
      * @see [[RddOfTriplesOps.add]]
      */
    def add(triple: Triple): RDD[Triple] =
      RddOfTriplesOps.add(triples, triple)

    /**
      * Add a list of statements to the current RDF graph.
      *
      * @param triple the list of triples to be added.
      * @return new RDD of triples containing this list of statements.
      * @see [[RddOfTriplesOps.addAll]]
      */
    def addAll(triple: Seq[Triple]): RDD[Triple] =
      RddOfTriplesOps.addAll(triples, triple)

    /**
      * Removes a statement from the current RDF graph.
      * The statement with the same subject, predicate and object as that supplied will be removed from the model.
      *
      * @param triple the statement to be removed.
      * @return new RDD of triples without this statement.
      * @see [[RddOfTriplesOps.remove]]
      */
    def remove(triple: Triple): RDD[Triple] =
      RddOfTriplesOps.remove(triples, triple)

    /**
      * Removes all the statements from the current RDF graph.
      * The statements with the same subject, predicate and object as those supplied will be removed from the model.
      *
      * @param triple the list of statements to be removed.
      * @return new RDD of triples without these statements.
      * @see [[RddOfTriplesOps.removeAll]]
      */
    def removeAll(triple: Seq[Triple]): RDD[Triple] =
      RddOfTriplesOps.removeAll(triples, triple)

    /**
      * Write N-Triples from a given RDD of triples
      *
      * @param path path to the file containing N-Triples
      * @see [[RddOfTriplesOps.saveAsNTriplesFile]]
      */
    def saveAsNTriplesFile(path: String): Unit =
      RddOfTriplesOps.saveAsNTriplesFile(triples, path)


    def toGraph(graph: JenaGraph): JenaGraph = RddOfTriplesOps.toGraph(graph, triples)

    def toGraph(): JenaGraph = toGraph(GraphFactory.createDefaultGraph)

    def toModel(model: Model): Model = {
      toGraph(model.getGraph)
      model
    }

    def toModel(): Model = toModel(ModelFactory.createDefaultModel)
  }

  /**
    * Adds all methods to [[RDD]] that allows to use HDT TripleOps functions.
    */
  implicit class HDTTripleOperations(triples: RDD[Triple]) extends Logging {

    import net.sansa_stack.rdf.spark.model.hdt.TripleOps

    type HDTDataFrame = DataFrame

    /**
      * Convert an RDD of triples into a DataFrame of hdt.
      *
      * @return a DataFrame of hdt triples.
      */
    def asHDT(): HDTDataFrame =
      TripleOps.asHDT(triples)

  }

  /**
    * Adds methods, `readHDTFromDist`, to [[SparkSession]] that allows to read
    * hdt files.
    */
  implicit class HDTReader(spark: SparkSession) {

    import net.sansa_stack.rdf.spark.model.hdt.TripleOps

    /**
      * Read hdt data from disk.
      *
      * @param input -- path to hdt data.
      * @retun DataFrame of hdt, subject, predicate, and object view.
      */
    def readHDTFromDisk(input: String): (DataFrame, DataFrame, DataFrame, DataFrame) =
      TripleOps.readHDTFromDisk(input)
  }

  /**
    * Adds methods, `readHDTFromDist`, to [[SparkSession]] that allows to read
    * hdt files.
    */
  implicit class HDTWriter(hdt_tables: (DataFrame, DataFrame, DataFrame, DataFrame)) {

    import net.sansa_stack.rdf.spark.model.hdt.TripleOps

    /**
      * Function saves the Index and Dictionaries Dataframe into given path
      *
      * @param output path to be written
      */
    def saveAsCSV(output: String): Unit =
      TripleOps.saveAsCSV(hdt_tables._1, hdt_tables._2, hdt_tables._3, hdt_tables._4,
        output, org.apache.spark.sql.SaveMode.Overwrite)
  }

  /**
    * Adds all methods to [[RDD]] that allows to use Tensor TripleOps functions.
    */
  implicit class TensorTripleOperations(triples: RDD[Triple]) extends Logging {

    import net.sansa_stack.rdf.spark.model.tensor.TripleOps

    /**
      * Return all the mapped triples (tensor) based on their relations
      *
      * @return all the mapped triples (tensor) based on their relations
      */
    def asTensor(): RDD[(Long, Long, Long)] =
      TripleOps.getMappedTriples(triples)

    /**
      * Return size of the entities in the graph
      *
      * @return size of the entities in the graph
      */
    def getNumEntities(): Long =
      TripleOps.getNumEntities(triples)

    /**
      * Return size of the relations in the graph
      *
      * @return size of the relations in the graph
      */
    def getNumRelations(): Long =
      TripleOps.getNumRelations(triples)

  }

  /**
    * Adds all methods to [[DataFrame]] that allows to use TripleOps functions.
    */
  implicit class DFTripleOperations(triples: DataFrame) extends Logging {

    import net.sansa_stack.rdf.spark.model.df.TripleOps

    /**
      * Convert a DataFrame of triples into [[RDD[Triple]]].
      *
      * @return a DataFrame of triples.
      */
    def toRDD(): RDD[Triple] =
      TripleOps.toRDD(triples)

    /**
      * Convert an DataFrame of triples into a Dataset of Triple.
      *
      * @return a Dataset of triples.
      */
    def toDS(): Dataset[Triple] =
      TripleOps.toDS(triples)

    /**
      * Get triples.
      *
      * @return [[RDD[Triple]]] which contains list of the triples.
      */
    def getTriples(): DataFrame =
      TripleOps.getTriples(triples)

    /**
      * Get subjects.
      *
      * @return DataFrame which contains list of the subjects.
      */
    def getSubjects(): DataFrame =
      TripleOps.getSubjects(triples)

    /**
      * Get predicates.
      *
      * @return DataFrame which contains list of the predicates.
      */
    def getPredicates(): DataFrame =
      TripleOps.getPredicates(triples)

    /**
      * Get objects.
      *
      * @return DataFrame which contains list of the objects.
      */
    def getObjects(): DataFrame =
      TripleOps.getObjects(triples)

    /**
      * Returns an DataFrame of triples that match with the given input.
      *
      * @param subject   the subject
      * @param predicate the predicate
      * @param object    the object
      * @return DataFrame of triples
      */
    def find(subject: Option[String] = None, predicate: Option[String] = None, `object`: Option[String] = None): DataFrame =
      TripleOps.find(triples, subject, predicate, `object`)

    /**
      * Returns an DataFrame of triples that match with the given input.
      *
      * @param triple the triple to be checked
      * @return DataFrame of triples that match the given input
      */
    def find(triple: Triple): DataFrame =
      TripleOps.find(triples, triple)

    /**
      * Return the union all of RDF graphs.
      *
      * @param others sequence of DataFrames of other RDF graph
      * @return graph (union of all)
      */
    def unionAll(others: Seq[DataFrame]): DataFrame =
      TripleOps.unionAll(triples, others)

    /**
      * Determine whether this RDF graph contains any triples
      * with a given (subject, predicate, object) pattern.
      *
      * @param subject   the subject (None for any)
      * @param predicate the predicate (None for any)
      * @param object    the object (None for any)
      * @return true if there exists within this RDF graph
      *         a triple with (S, P, O) pattern, false otherwise
      */
    def contains(subject: Option[String] = None, predicate: Option[String] = None, `object`: Option[String] = None): Boolean =
      TripleOps.contains(triples, subject, predicate, `object`)

    /**
      * Determine if a triple is present in this RDF graph.
      *
      * @param triple the triple to be checked
      * @return true if the statement s is in this RDF graph, false otherwise
      */
    def contains(triple: Triple): Boolean =
      TripleOps.contains(triples, triple)

    /**
      * Determine if any of the triples in an RDF graph are also contained in this RDF graph.
      *
      * @param other the other RDF graph containing the statements to be tested
      * @return true if any of the statements in RDF graph are also contained
      *         in this RDF graph and false otherwise.
      */
    def containsAny(other: DataFrame): Boolean =
      TripleOps.containsAny(triples, other)

    /**
      * Determine if all of the statements in an RDF graph are also contained in this RDF graph.
      *
      * @param other the other RDF graph containing the statements to be tested
      * @return true if all of the statements in RDF graph are also contained
      *         in this RDF graph and false otherwise.
      */
    def containsAll(other: DataFrame): Boolean =
      TripleOps.containsAll(triples, other)

    /**
      * Add a statement to the current RDF graph.
      *
      * @param triple the triple to be added.
      * @return new DataFrame of triples containing this statement.
      */
    def add(triple: Triple): DataFrame =
      TripleOps.add(triples, triple)

    /**
      * Add a list of statements to the current RDF graph.
      *
      * @param triple the list of triples to be added.
      * @return new DataFrame of triples containing this list of statements.
      */
    def addAll(triple: Seq[Triple]): DataFrame =
      TripleOps.addAll(triples, triple)

    /**
      * Removes a statement from the current RDF graph.
      * The statement with the same subject, predicate and
      * object as that supplied will be removed from the model.
      *
      * @param triple the statement to be removed.
      * @return new DataFrame of triples without this statement.
      */
    def remove(triple: Triple): DataFrame =
      TripleOps.remove(triples, triple)

    /**
      * Removes all the statements from the current RDF graph.
      * The statements with the same subject, predicate and
      * object as those supplied will be removed from the model.
      *
      * @param triple the list of statements to be removed.
      * @return new DataFrame of triples without these statements.
      */
    def removeAll(triple: Seq[Triple]): DataFrame =
      TripleOps.removeAll(triples, triple)

    /**
      * Write N-Triples from a given DataFrame of triples
      *
      * @param path    path to the file containing N-Triples
      */
    def saveAsNTriplesFile(path: String): Unit =
      TripleOps.saveAsNTriplesFile(triples, path)
  }

  /**
    * Adds all methods to [[Dataset[Triple]]] that allows to use TripleOps functions.
    */
  implicit class DSTripleOperations(triples: Dataset[Triple]) extends Logging {

    import net.sansa_stack.rdf.spark.model.ds.TripleOps

    /**
      * Convert a Dataset of triples into [[RDD[Triple]]].
      *
      * @return a RDD of triples.
      */
    def toRDD(): RDD[Triple] =
      TripleOps.toRDD(triples)

    /**
      * Convert an Dataset of triples into a DataFrame of triples.
      *
      * @return a DataFrame of triples.
      */
    def toDF(): DataFrame =
      TripleOps.toDF(triples)

    /**
      * Get triples.
      *
      * @return [[Dataset[Triple]]] which contains list of the triples.
      */
    def getTriples(): Dataset[Triple] =
      TripleOps.getTriples(triples)

    /**
      * Returns an Dataset of triples that match with the given input.
      *
      * @param subject   the subject
      * @param predicate the predicate
      * @param object    the object
      * @return Dataset of triples
      */
    def find(subject: Option[Node] = None, predicate: Option[Node] = None, `object`: Option[Node] = None): Dataset[Triple] =
      TripleOps.find(triples, subject, predicate, `object`)

    /**
      * Returns an Dataset of triples that match with the given input.
      *
      * @param triple the triple to be checked
      * @return Dataset of triples that match the given input
      */
    def find(triple: Triple): Dataset[Triple] =
      TripleOps.find(triples, triple)

    /**
      * Return the union all of RDF graphs.
      *
      * @param others sequence of Dataset of other RDF graph
      * @return graph (union of all)
      */
    def unionAll(others: Seq[Dataset[Triple]]): Dataset[Triple] =
      TripleOps.unionAll(triples, others)

    /**
      * Returns a new RDF graph that contains the intersection
      * of the current RDF graph with the given RDF graph.
      *
      * @param other the other RDF graph
      * @return the intersection of both RDF graphs
      */
    def intersection(other: Dataset[Triple]): Dataset[Triple] =
      TripleOps.intersection(triples, other)

    /**
      * Returns a new RDF graph that contains the difference
      * between the current RDF graph and the given RDF graph.
      *
      * @param other the other RDF graph
      * @return the difference of both RDF graphs
      */
    def difference(other: Dataset[Triple]): Dataset[Triple] =
      TripleOps.difference(triples, other)

    /**
      * Determine whether this RDF graph contains any triples
      * with a given (subject, predicate, object) pattern.
      *
      * @param subject   the subject (None for any)
      * @param predicate the predicate (None for any)
      * @param object    the object (None for any)
      * @return true if there exists within this RDF graph
      *         a triple with (S, P, O) pattern, false otherwise
      */
    def contains(subject: Option[Node] = None, predicate: Option[Node] = None, `object`: Option[Node] = None): Boolean =
      TripleOps.contains(triples, subject, predicate, `object`)

    /**
      * Determine if a triple is present in this RDF graph.
      *
      * @param triple the triple to be checked
      * @return true if the statement s is in this RDF graph, false otherwise
      */
    def contains(triple: Triple): Boolean =
      TripleOps.contains(triples, triple)

    /**
      * Determine if any of the triples in an RDF graph are also contained in this RDF graph.
      *
      * @param other the other RDF graph containing the statements to be tested
      * @return true if any of the statements in RDF graph are also contained
      *         in this RDF graph and false otherwise.
      */
    def containsAny(other: Dataset[Triple]): Boolean =
      TripleOps.containsAny(triples, other)

    /**
      * Determine if all of the statements in an RDF graph are also contained in this RDF graph.
      *
      * @param other the other RDF graph containing the statements to be tested
      * @return true if all of the statements in RDF graph are also contained
      *         in this RDF graph and false otherwise.
      */
    def containsAll(other: Dataset[Triple]): Boolean =
      TripleOps.containsAll(triples, other)

    /**
      * Add a statement to the current RDF graph.
      *
      * @param triple the triple to be added.
      * @return new Dataset of triples containing this statement.
      */
    def add(triple: Triple): Dataset[Triple] =
      TripleOps.add(triples, triple)

    /**
      * Add a list of statements to the current RDF graph.
      *
      * @param triple the list of triples to be added.
      * @return new Dataset of triples containing this list of statements.
      */
    def addAll(triple: Seq[Triple]): Dataset[Triple] =
      TripleOps.addAll(triples, triple)

    /**
      * Removes a statement from the current RDF graph.
      * The statement with the same subject, predicate and
      * object as that supplied will be removed from the model.
      *
      * @param triple the statement to be removed.
      * @return new Dataset of triples without this statement.
      */
    def remove(triple: Triple): Dataset[Triple] =
      TripleOps.remove(triples, triple)

    /**
      * Removes all the statements from the current RDF graph.
      * The statements with the same subject, predicate and
      * object as those supplied will be removed from the model.
      *
      * @param triple the list of statements to be removed.
      * @return new Dataset of triples without these statements.
      */
    def removeAll(triple: Seq[Triple]): Dataset[Triple] =
      TripleOps.removeAll(triples, triple)

    /**
      * Write N-Triples from a given Dataset of triples
      *
      * @param path    path to the file containing N-Triples
      */
    def saveAsNTriplesFile(path: String): Unit =
      TripleOps.saveAsNTriplesFile(triples, path)
  }

  /**
    * Adds methods, `asGraph` to [[RDD]] that allows to transform as a GraphX representation.
    */
  implicit class GraphLoader(triples: RDD[Triple]) extends Logging {

    import net.sansa_stack.rdf.spark.model.graph.GraphOps

    /**
      * Constructs GraphX graph from RDD of triples
      *
      * @return object of GraphX which contains the constructed  ''graph''.
      * @see [[net.sansa_stack.rdf.spark.model.graph.GraphOps.constructGraph]]
      */
    def asGraph(): Graph[Node, Node] =
      GraphOps.constructGraph(triples)

    /**
      * Constructs Hashed GraphX graph from RDD of triples
      *
      * @return object of GraphX which contains the constructed hashed ''graph''.
      * @see [[net.sansa_stack.rdf.spark.model.graph.GraphOps.constructHashedGraph]]
      */
    def asHashedGraph(): Graph[Node, Node] =
      GraphOps.constructHashedGraph(triples)

    /**
      * Constructs String GraphX graph from RDD of triples
      *
      * @return object of GraphX which contains the constructed string ''graph''.
      * @see [[net.sansa_stack.rdf.spark.model.graph.GraphOps.constructStringGraph]]
      */
    def asStringGraph(): Graph[String, String] =
      GraphOps.constructStringGraph(triples)
  }

  /**
    * Adds methods, `astTriple`, `find`, `size` to [[Graph][Node, Node]] that allows to different operations to it.
    */
  implicit class GraphOperations(graph: Graph[Node, Node]) extends Logging {

    import net.sansa_stack.rdf.spark.model.graph.GraphOps

    /**
      * Convert a graph into a RDD of Triple.
      *
      * @return a RDD of triples.
      * @see [[net.sansa_stack.rdf.spark.model.graph.GraphOps.toRDD]]
      */
    def toRDD(): RDD[Triple] =
      GraphOps.toRDD(graph)

    /**
      * Convert a graph into a DataFrame.
      *
      * @return a DataFrame of triples.
      * @see [[net.sansa_stack.rdf.spark.model.graph.GraphOps.toDF]]
      */
    def toDF(): DataFrame =
      GraphOps.toDF(graph)

    /**
      * Convert a graph into a Dataset of Triple.
      *
      * @return a Dataset of triples.
      * @see [[net.sansa_stack.rdf.spark.model.graph.GraphOps.toDS]]
      */
    def toDS(): Dataset[Triple] =
      GraphOps.toDS(graph)

    /**
      * Finds triplets  of a given graph.
      *
      * @param subject
      * @param predicate
      * @param object
      * @return graph which contains subset of the reduced graph.
      * @see [[net.sansa_stack.rdf.spark.model.graph.GraphOps.find]]
      */
    def find(subject: Node, predicate: Node, `object`: Node): Graph[Node, Node] =
      GraphOps.find(graph, subject, predicate, `object`)

    /**
      * Gets triples of a given graph.
      *
      * @return [[[RDD[Triple]]] from the given graph.
      * @see [[net.sansa_stack.rdf.spark.model.graph.GraphOps.getTriples]]
      */
    def getTriples(): RDD[Triple] =
      GraphOps.getTriples(graph)

    /**
      * Gets subjects of a given graph.
      *
      * @return [[[RDD[Node]]] from the given graph.
      * @see [[net.sansa_stack.rdf.spark.model.graph.GraphOps.getSubjects]]
      */
    def getSubjects(): RDD[Node] =
      GraphOps.getSubjects(graph)

    /**
      * Gets predicates of a given graph.
      *
      * @return [[[RDD[Node]]] from the given graph.
      * @see [[net.sansa_stack.rdf.spark.model.graph.GraphOps.getPredicates]]
      */
    def getPredicates(): RDD[Node] =
      GraphOps.getPredicates(graph)

    /**
      * Gets objects of a given graph.
      *
      * @return [[[RDD[Node]]] from the given graph.
      * @see [[net.sansa_stack.rdf.spark.model.graph.GraphOps.getObjects]]
      */
    def getObjects(): RDD[Node] =
      GraphOps.getObjects(graph)

    /**
      * Filter out the subject from a given graph,
      * based on a specific function @func .
      *
      * @param func a partial funtion.
      * @return [[Graph[Node, Node]]] a subset of the given graph.
      * @see [[net.sansa_stack.rdf.spark.model.graph.GraphOps.filterSubjects]]
      */
    def filterSubjects(func: Node => Boolean): Graph[Node, Node] =
      GraphOps.filterSubjects(graph, func)

    /**
      * Filter out the predicates from a given graph,
      * based on a specific function @func .
      *
      * @param func a partial funtion.
      * @return [[Graph[Node, Node]]] a subset of the given graph.
      * @see [[net.sansa_stack.rdf.spark.model.graph.GraphOps.filterPredicates]]
      */
    def filterPredicates(func: Node => Boolean): Graph[Node, Node] =
      GraphOps.filterPredicates(graph, func)

    /**
      * Filter out the objects from a given graph,
      * based on a specific function @func .
      *
      * @param func a partial funtion.
      * @return [[Graph[Node, Node]]] a subset of the given graph.
      * @see [[net.sansa_stack.rdf.spark.model.graph.GraphOps.filterObjects]]
      */
    def filterObjects(func: Node => Boolean): Graph[Node, Node] =
      GraphOps.filterObjects(graph, func)

    /**
      * Compute the size of the graph
      *
      * @return the number of edges in the graph.
      * @see [[net.sansa_stack.rdf.spark.model.graph.GraphOps.size]]
      */
    def size(): Long =
      GraphOps.size(graph)

    /**
      * Return the union of this graph and another one.
      *
      * @param other of the other graph
      * @return graph (union of all)
      * @see [[net.sansa_stack.rdf.spark.model.graph.GraphOps.union]]
      */
    def union(other: Graph[Node, Node]): Graph[Node, Node] =
      GraphOps.union(graph, other)

    /**
      * Returns a new RDF graph that contains the intersection of the current RDF graph with the given RDF graph.
      *
      * @param other the other RDF graph
      * @return the intersection of both RDF graphs
      * @see [[net.sansa_stack.rdf.spark.model.graph.GraphOps.difference]]
      */
    def difference(other: Graph[Node, Node]): Graph[Node, Node] =
      GraphOps.difference(graph, other)

    /**
      * Returns a new RDF graph that contains the difference between the current RDF graph and the given RDF graph.
      *
      * @param other the other RDF graph
      * @return the difference of both RDF graphs
      * @see [[net.sansa_stack.rdf.spark.model.graph.GraphOps.intersection]]
      */
    def intersection(other: Graph[Node, Node]): Graph[Node, Node] =
      GraphOps.intersection(graph, other)

    /**
      * Returns the lever at which vertex stands in the hierarchy.
      */
    def hierarcyDepth(): Graph[(VertexId, Int, Node), Node] =
      GraphOps.hierarcyDepth(graph)

    /**
      * Save the Graph to JSON format
      *
      * @param output the output
      */
    def saveGraphToJson(output: String): Unit =
      GraphOps.saveGraphToJson(graph, output)
  }

}
