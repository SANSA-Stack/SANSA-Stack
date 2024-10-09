package net.sansa_stack.ml.spark.utils

import scala.collection.JavaConverters._

import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.query.Query
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.lang.ParserSPARQL11
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * A SPARQL query transformer instance takes a SPARQL query string as
  * constructor argument and can than be invoked (via the transform method) on
  * a Dataset[Triple] object returning a DataFrame which contains one column
  * for each SPARQL projection variable.
  *
  * The SPARQL query transformer only works with SELECT queries.
  *
  * @param queryString String containing the SPARQL select query to run on a
  *                    Dataset[Triple] object
  */
abstract class SPARQLQuery(queryString: String) extends Transformer {
  override val uid: String = Identifiable.randomUID("sparqlQuery")

  protected val spark = SparkSession.builder().getOrCreate()

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  protected def getResultBindings(dataset: Dataset[_]): RDD[Binding] = {
    // convert Dataset to RDD[Triple] // TODO why do we need this?
    implicit val tripleEncoder = Encoders.kryo(classOf[Triple])
    val triplesRDD = dataset.as[Triple].rdd

    // create query engine
    import net.sansa_stack.query.spark._
    val qef = triplesRDD.sparql(SPARQLEngine.Ontop)

    // create query execution
    val qe = qef.createQueryExecution(queryString)

    // compute bindings
    val bindings: RDD[Binding] = qe.execSelectSpark().getBindings

    bindings
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(StructField("value", DataTypes.BinaryType)))
  }
}

private class SPARQLQuery1(queryString: String, projectionVar: Var)
  extends SPARQLQuery(queryString) {

  override def transform(dataset: Dataset[_]): DataFrame = {
    val bindings: RDD[Binding] = getResultBindings(dataset)

    val bindingVals: RDD[Node] = bindings.map(binding => {
      binding.get(projectionVar)
    })

    implicit val nodeEncoder = Encoders.kryo(classOf[Node])
    import spark.implicits._

    bindingVals.toDF()
  }
}

private class SPARQLQuery2(queryString: String, projectionVariables: (Var, Var))
  extends SPARQLQuery(queryString) {

  override def transform(dataset: Dataset[_]): DataFrame = {
    val bindings: RDD[Binding] = getResultBindings(dataset)

    val bindingVals: RDD[(Node, Node)] = bindings.map(binding => {
      (binding.get(projectionVariables._1),
        binding.get(projectionVariables._2))
    })

    implicit val nodeTupleEncoder = Encoders.kryo(classOf[(Node, Node)])
    import spark.implicits._

    bindingVals.toDF()
  }
}

private class SPARQLQuery3(queryString: String, projectionVariables: (Var, Var, Var))
  extends SPARQLQuery(queryString) {

  override def transform(dataset: Dataset[_]): DataFrame = {
    val bindings = getResultBindings(dataset)

    val bindingVals: RDD[(Node, Node, Node)] = bindings.map(binding => {
      (binding.get(projectionVariables._1),
        binding.get(projectionVariables._2),
        binding.get(projectionVariables._3))
    })

    implicit val nodeTupleEncoder = Encoders.kryo(classOf[(Node, Node, Node)])
    import spark.implicits._

    bindingVals.toDF()
  }
}

private class SPARQLQuery4(queryString: String, projectionVariables: (Var, Var, Var, Var))
  extends SPARQLQuery(queryString) {

  override def transform(dataset: Dataset[_]): DataFrame = {
    val bindings = getResultBindings(dataset)

    val bindingVals: RDD[(Node, Node, Node, Node)] = bindings.map(binding => {
      (binding.get(projectionVariables._1),
        binding.get(projectionVariables._2),
        binding.get(projectionVariables._3),
        binding.get(projectionVariables._4))
    })

    implicit val nodeTupleEncoder = Encoders.kryo(classOf[(Node, Node, Node, Node)])
    import spark.implicits._

    bindingVals.toDF()
  }
}

private class SPARQLQuery5(queryString: String, projectionVariables: (Var, Var, Var, Var, Var))
  extends SPARQLQuery(queryString) {

  override def transform(dataset: Dataset[_]): DataFrame = {
    val bindings = getResultBindings(dataset)

    val bindingVals: RDD[(Node, Node, Node, Node, Node)] = bindings.map(binding => {
      (binding.get(projectionVariables._1),
        binding.get(projectionVariables._2),
        binding.get(projectionVariables._3),
        binding.get(projectionVariables._4),
        binding.get(projectionVariables._5))
    })

    implicit val nodeTupleEncoder = Encoders.kryo(classOf[(Node, Node, Node, Node, Node)])
    import spark.implicits._

    bindingVals.toDF()
  }
}

object SPARQLQuery {
  def apply(queryString: String): SPARQLQuery = {
    val parser = new ParserSPARQL11()

    val query: Query = parser.parse(new Query(), queryString)

    val projectionVars: Seq[Var] = query.getProjectVars.asScala.toList

    projectionVars.size match {
      case 1 => new SPARQLQuery1(queryString, projectionVars.head)
      case 2 => new SPARQLQuery2(
        queryString, (projectionVars.head, projectionVars(1)))
      case 3 => new SPARQLQuery3(
        queryString,
        (projectionVars.head,
          projectionVars(1),
          projectionVars(2)))
      case 4 => new SPARQLQuery4(
        queryString,
        (projectionVars.head,
          projectionVars(1),
          projectionVars(2),
          projectionVars(3)))
      case 5 => new SPARQLQuery5(
        queryString,
        (projectionVars.head,
          projectionVars(1),
          projectionVars(2),
          projectionVars(3),
          projectionVars(4)))
      case _ => throw new NotImplementedError()
    }
  }
}



