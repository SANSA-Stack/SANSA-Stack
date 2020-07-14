package net.sansa_stack.query.spark.ontop;

import scala.collection.JavaConverters._

import org.aksw.jena_sparql_api.core.{QueryExecutionBaseSelect, QueryExecutionFactory, ResultSetCloseable}
import org.aksw.jena_sparql_api.utils.ResultSetUtils
import org.apache.jena.graph.GraphUtil
import org.apache.jena.query.{Query, QueryExecution}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.sparql.graph.GraphFactory

class QueryExecutionOntopSpark(query: Query, subFactory: QueryExecutionFactory, ontop: OntopSPARQLEngine)
	extends QueryExecutionBaseSelect(query, subFactory) {

	override def executeCoreSelect(query: Query): ResultSetCloseable = {
		val bindings = ontop.execSelect(query.toString()).collect().iterator

		val tmp = ResultSetUtils.create2(query.getProjectVars, bindings.asJava)

		new ResultSetCloseable(tmp);
	}

	override def execAsk(): Boolean = ontop.execAsk(query.toString())

	override def execConstruct(): Model = {
		val triples = ontop.execConstruct(query.toString()).collect()
		val g = GraphFactory.createDefaultGraph()
		GraphUtil.add(g, triples)
		ModelFactory.createModelForGraph(g)
	}

	override def executeCoreSelectX(query: Query): QueryExecution = throw new UnsupportedOperationException
}
