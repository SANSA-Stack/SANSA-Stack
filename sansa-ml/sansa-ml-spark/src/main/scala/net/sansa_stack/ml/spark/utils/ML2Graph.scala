package net.sansa_stack.ml.spark.utils

import java.util.{Calendar, Date}

import net.sansa_stack.rdf.spark.model.TripleOperations
import net.sansa_stack.rdf.spark.model.ds.TripleOps.spark
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable


class ML2Graph {

  protected var entityColumns: Array[String] = null
  protected var valueColumn: String = null

  protected var elementPropertyURIasString: String = "sansa-stack/sansaVocab/element"
  protected var valuePropertyURIasString: String = "sansa-stack/sansaVocab/value"
  protected var predictionPropertyURIasString: String = "sansa-stack/sansaVocab/prediction"
  protected var experimentTypePropertyURIasString: String = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

  protected var experimentTypeURIasString: String = "sansa-stack/sansaVocab/experiment"

  val comment: String = null

  def setValueColumn(valueColumnName: String): this.type = {
    valueColumn = valueColumnName
    this
  }

  def setEntityColumns(entityColumnNames: Array[String]): this.type = {
    entityColumns = entityColumnNames
    this
  }

  def setEntityColumn(entityColumnName: String): this.type = {
    entityColumns = Array(entityColumnName)
    this
  }

  def transform(dataset: DataFrame): RDD[Triple] = {
    // create experiment node
    val metagraphDatetime: Date = Calendar.getInstance().getTime()
    val experimentHash = metagraphDatetime.toString.hashCode.toString
    val experimentNode = NodeFactory.createBlankNode(experimentHash)

    val experimentTypePropertyNode = NodeFactory.createURI(experimentTypePropertyURIasString)
    val experimentTypeNode = NodeFactory.createURI(experimentTypeURIasString)

    // get value datatype
    val valueDatatype = dataset.schema(valueColumn).dataType match {
      case StringType => XSDDatatype.XSDstring
      case DoubleType => XSDDatatype.XSDdouble
      case IntegerType => XSDDatatype.XSDint
    }
    println(valueDatatype)

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
    val metagraph: RDD[Triple] = dataset
      .rdd
      .flatMap(r => {
        val row = r.asInstanceOf[Row]
        val entityNames: Array[String] = entityColumns.map(row.getAs[String](_))
        val entityNodes: Array[Node] = entityNames.map(NodeFactory.createURI(_))
        val value: String = row.getAs[String](valueColumn)
        val valueNode = NodeFactory.createLiteralByValue(value, valueDatatype)

        val predictionNode: Node = NodeFactory.createBlankNode(experimentHash + entityNames.mkString("").hashCode)

        val predictionPropertyNode: Node = NodeFactory.createURI(predictionPropertyURIasString)
        val elementPropertyNode: Node = NodeFactory.createURI(elementPropertyURIasString)
        val valuePropertyURINode: Node = NodeFactory.createURI(valuePropertyURIasString)
        val experimentTypePropertyNode: Node = NodeFactory.createURI(experimentTypePropertyURIasString)

        // entity nodes to prediction blank node
        val triples: mutable.ArrayBuffer[Triple] = mutable.ArrayBuffer(entityNodes.map(
          en =>
          Triple.create(
            predictionNode,
            elementPropertyNode,
            en
          )
        ))

        // prediction blank node to overall experiment
        triples.append(
          Triple.create(
            experimentNode,
            predictionPropertyNode,
            predictionNode
          )
        )

        // prediction blank node to predicted value
        triples.append(
          Triple.create(
            experimentNode,
            valuePropertyURINode,
            valueNode
          )
        )
        triples

      })
      .union(centralNodeTriples)

    metagraph

  }
}
