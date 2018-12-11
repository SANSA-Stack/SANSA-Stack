package net.sansa_stack.datalake.spark

import java.util

import com.google.common.collect.ArrayListMultimap

import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer, Set}

/**
  * Created by mmami on 07.03.18.
  */
trait QueryExecutor[T] { // T is a ParSet (Parallel dataSet)

    /* Generates a ParSet with the number of filters (on predicates) in the star */
    def query(sources : Set[(HashMap[String, String], String, String)],
              optionsMap: HashMap[String, (Map[String, String],String)],
              toJoinWith: Boolean,
              star: String,
              prefixes: Map[String, String],
              select: util.List[String],
              star_predicate_var: mutable.HashMap[(String, String), String],
              neededPredicates: Set[String],
              filters: ArrayListMultimap[String, (String, String)],
              leftJoinTransformations: (String, Array[String]),
              rightJoinTransformations: Array[String],
              joinPairs: Map[(String,String), String]
             ) : (T, Integer)

    /* Transforms a ParSet to another ParSet based on the SPARQL TRANSFORM clause */
    def transform(ps: Any, column: String, transformationsArray : Array[String]): Any

    /* Print the schema of the ParSet */
    def join(joins: ArrayListMultimap[String, (String, String)], prefixes: Map[String, String], star_df: Map[String, T]): T

    /* Generates a new ParSet projecting out one or more attributes */
    def project(jDF: Any, columnNames: Seq[String], distinct: Boolean): T

    /* Counts the number of tuples of a ParSet */
    def count(joinPS: T): Long

    /* Sort tuples of a ParSet based on an attribute variable */
    def orderBy(joinPS: Any, direction: String, variable: String): T

    /* Group attributes based on aggregates function(s) */
    def groupBy(joinPS: Any, groupBys: (ListBuffer[String], Set[(String, String)])): T

    /* Return the first 'limitValue' values of the ParSet */
    def limit(joinPS: Any, limitValue: Int) : T

    /* Show some results */
    def show(PS: Any)

    /* Compute the results */
    def run(jDF: Any)
}
