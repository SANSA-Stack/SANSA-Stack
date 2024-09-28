package net.sansa_stack.query.spark.api.impl;

import net.sansa_stack.query.spark.api.domain.JavaQueryExecutionSpark;
import net.sansa_stack.query.spark.api.domain.JavaResultSetSpark;
import org.aksw.jena_sparql_api.core.QueryExecutionBaseSelect;
import org.aksw.jenax.arq.util.binding.ResultSetUtils;
import org.aksw.jenax.dataaccess.sparql.factory.execution.query.QueryExecutionFactory;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetCloseable;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.modify.TemplateLib;
import org.apache.jena.sparql.syntax.Template;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Iterator;
import java.util.List;

public abstract class JavaQueryExecutionSparkBase
        extends QueryExecutionBaseSelect
        implements JavaQueryExecutionSpark {

    protected SparkSession sparkSession;

    public JavaQueryExecutionSparkBase(Query query, QueryExecutionFactory subFactory, SparkSession sparkSession) {
        super(query, subFactory);
        this.sparkSession = sparkSession;
    }

    @Override
    public JavaRDD<Triple> execConstructSparkJava() {
        Template template = query.getConstructTemplate();
        List<Triple> triples = template.getTriples();

        JavaRDD<Triple> result = execSelectSparkJava().getRdd()
                .mapPartitions(bindingIt -> TemplateLib.calcTriples(triples, bindingIt))
                .distinct();

        return result;
    }

    @Override
    public JavaRDD<Quad> execConstructQuadsSparkJava() {
        Template template = query.getConstructTemplate();
        List<Quad> quads = template.getQuads();

        JavaRDD<Quad> result = execSelectSparkJava().getRdd()
                .mapPartitions(bindingIt -> TemplateLib.calcQuads(quads, bindingIt))
                .distinct();

        return result;
    }

    @Override
    protected ResultSetCloseable executeCoreSelect(Query query) {
        JavaResultSetSpark rs = execSelectSparkJava();
        Iterator<Binding> it = rs.getRdd().collect().iterator();//.toLocalIterator();

        ResultSet tmp = ResultSetUtils.createUsingVars(rs.getResultVars(), it);
        ResultSetCloseable result = new ResultSetCloseable(tmp, this);
        return result;
    }

    @Override
    protected QueryExecution executeCoreSelectX(Query query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTimeout1() {
        return -1;
    }

    @Override
    public JsonArray execJson() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<JsonObject> execJsonItems() {
        throw new UnsupportedOperationException();
    }
}
