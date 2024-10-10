package net.sansa_stack.ml.spark.utils

import java.util.{Calendar, Date}

import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DecimalType, DoubleType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class ML2Graph {

  protected var _entityColumns: Array[String] = null
  protected var _valueColumn: String = null

  val _comment: String = null

  def setValueColumn(valueColumnName: String): this.type = {
    _valueColumn = valueColumnName
    this
  }

  def setEntityColumns(entityColumnNames: Array[String]): this.type = {
    _entityColumns = entityColumnNames
    this
  }

  def setEntityColumn(entityColumnName: String): this.type = {
    _entityColumns = Array(entityColumnName)
    this
  }

  def transform(df: Dataset[_]): RDD[Triple] = {

    val spark = SparkSession.builder().getOrCreate()
    implicit val rdfTripleEncoder: Encoder[Triple] = org.apache.spark.sql.Encoders.kryo[Triple]

    val b_valueColumn: Broadcast[String] = spark.sparkContext.broadcast(_valueColumn)
    val b_entityColumns: Broadcast[Array[String]] = spark.sparkContext.broadcast(_entityColumns)

    // get value datatype
    val valueDatatype = df.schema(b_valueColumn.value).dataType match {
      case StringType => XSDDatatype.XSDstring
      case DoubleType => XSDDatatype.XSDdouble
      case IntegerType => XSDDatatype.XSDint
      // case DecimalType => XSDDatatype.XSDdouble
      case _ => XSDDatatype.XSDstring
    }

    val b_valueDatatype = spark.sparkContext.broadcast(valueDatatype)



    // strings for URIs
    var _elementPropertyURIasString: String = "https://sansa-stack/sansaVocab/element"
    var _valuePropertyURIasString: String = "https://sansa-stack/sansaVocab/value"
    var _predictionPropertyURIasString: String = "https://sansa-stack/sansaVocab/prediction"
    var _experimentTypePropertyURIasString: String = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

    var _experimentTypeURIasString: String = "https://sansa-stack/sansaVocab/experiment"

    // create experiment node
    val metagraphDatetime: Date = Calendar.getInstance().getTime()
    val experimentHash: String = metagraphDatetime.toString.hashCode.toString
    val experimentNode: Node = NodeFactory.createBlankNode(experimentHash)

    val experimentTypePropertyNode: Node = NodeFactory.createURI(_experimentTypePropertyURIasString)
    val experimentTypeNode: Node = NodeFactory.createURI(_experimentTypeURIasString)



    // overall annotation
    // Create all inforamtion for this central node
    val centralNodeTriples: RDD[Triple] = spark.sqlContext.sparkContext.parallelize(List(
      Triple.create(
        experimentNode,
        experimentTypePropertyNode,
        experimentTypeNode
      )
    ))

    // transform dataframe to metagraph
    // now for the small triples:
    val metagraph: RDD[Triple] = df
      .rdd
      .flatMap(row => {
        val entityNames: Array[String] = b_entityColumns.value.map(row.asInstanceOf[Row].getAs[String](_)) /* if (b_entityColumns.value.size == 2) {
          Array(row.asInstanceOf[Row].getAs[String](0), row.asInstanceOf[Row].getAs[String](1))
        } else {
          Array(row.asInstanceOf[Row].getAs[String](0))
        } */

        val entityNodes: Array[Node] = entityNames.map(NodeFactory.createURI(_))
        val value: String = row.asInstanceOf[Row].getAs[Any](b_valueColumn.value).toString
        val valueNode = NodeFactory.createLiteralByValue(value, b_valueDatatype.value)

        val predictionNode: Node = NodeFactory.createBlankNode(experimentHash + entityNames.mkString("").hashCode)

        val predictionPropertyNode: Node = NodeFactory.createURI(_predictionPropertyURIasString)
        val elementPropertyNode: Node = NodeFactory.createURI(_elementPropertyURIasString)
        val valuePropertyURINode: Node = NodeFactory.createURI(_valuePropertyURIasString)
        // val experimentTypePropertyNode: Node = NodeFactory.createURI(experimentTypePropertyURIasString)

        // entity nodes to prediction blank node
        val entityNodeTriples: Array[Triple] = entityNodes.map(
          entityNode =>
          Triple.create(
            predictionNode,
            elementPropertyNode,
            entityNode
          )
        )

        // prediction blank node to overall experiment
        val valueExperimentTriples: Array[Triple] = Array(
          Triple.create(
            experimentNode,
            predictionPropertyNode,
            predictionNode
          ),
          Triple.create(
            predictionNode,
            valuePropertyURINode,
            valueNode
          )
        )
        entityNodeTriples ++ valueExperimentTriples
      })

    centralNodeTriples.union(metagraph)
  }
}
