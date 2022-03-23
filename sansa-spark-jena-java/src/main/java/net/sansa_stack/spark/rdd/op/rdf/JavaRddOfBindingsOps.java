package net.sansa_stack.spark.rdd.op.rdf;

import com.google.common.base.Preconditions;
import net.sansa_stack.spark.rdd.op.rx.JavaRddRxOps;
import net.sansa_stack.spark.util.JavaSparkContextUtils;
import org.aksw.commons.rx.function.RxFunction;
import org.aksw.jena_sparql_api.rx.query_flow.QueryFlowOps;
import org.aksw.jenax.arq.util.quad.QuadPatternUtils;
import org.aksw.jenax.arq.util.quad.QuadUtils;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.modify.TemplateLib;
import org.apache.jena.sparql.syntax.Template;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sparql.util.NodeFactoryExtra;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class JavaRddOfBindingsOps {

    /** The special ?ROWNUM variable supported by tarql */
    public static final Var ROWNUM = Var.alloc("ROWNUM");

    /**
     * Use an RDD of bindings as initial bindings for a construct query in order to yield triples.
     * This is conceptually the same approach as done by the tool 'tarql', hence the name.
     */
    public static JavaRDD<Triple> tarqlTriples(JavaRDD<Binding> rdd, Query query) {
        Preconditions.checkArgument(query.isConstructType(), "Construct query expected");

        JavaSparkContext cxt = JavaSparkContextUtils.fromRdd(rdd);
        Broadcast<Query> queryBc = cxt.broadcast(query);
        RxFunction<Binding, Triple> mapper = upstream -> {
            Query q = queryBc.getValue();
            Template template = q.getConstructTemplate();
            Op op = Algebra.compile(q);

            return upstream
                    .compose(QueryFlowOps.createMapperBindings(op))
                    .flatMap(QueryFlowOps.createMapperTriples(template)::apply);
        };

        rdd = enrichRddWithRowNumIfNeeded(rdd, query);
        return JavaRddRxOps.mapPartitions(rdd, mapper);
    }

    /**
     * Use an RDD of bindings as initial bindings for a construct query in order to yield quads.
     * This is conceptually the same approach as done by the tool 'tarql', hence the name.
     */
    public static JavaRDD<Quad> tarqlQuads(JavaRDD<Binding> rdd, Query query) {
        Preconditions.checkArgument(query.isConstructType(), "Construct query expected");

        JavaSparkContext cxt = JavaSparkContextUtils.fromRdd(rdd);
        Broadcast<Query> queryBc = cxt.broadcast(query);
        RxFunction<Binding, Quad> mapper = upstream -> {
            Query q = queryBc.getValue();
            Template template = q.getConstructTemplate();
            Op op = Algebra.compile(q);

            return upstream
                    .compose(QueryFlowOps.createMapperBindings(op))
                    .flatMap(QueryFlowOps.createMapperQuads(template)::apply);
        };

        rdd = enrichRddWithRowNumIfNeeded(rdd, query);
        return JavaRddRxOps.mapPartitions(rdd, mapper);
    }

    /** Each binding becomes its own dataset */
    public static JavaRDD<Dataset> tarqlDatasets(JavaRDD<Binding> rdd, Query query) {
        Preconditions.checkArgument(query.isConstructType(), "Construct query expected");

        JavaSparkContext cxt = JavaSparkContextUtils.fromRdd(rdd);
        Broadcast<Query> queryBc = cxt.broadcast(query);
        RxFunction<Binding, Dataset> mapper = upstream -> {
            Query q = queryBc.getValue();
            Template template = q.getConstructTemplate();
            Op op = Algebra.compile(q);

            return upstream
                    .compose(QueryFlowOps.createMapperBindings(op))
                    .flatMap(QueryFlowOps.createMapperQuads(template)::apply)
                    .reduceWith(DatasetGraphFactory::create, (dsg, quad) -> { dsg.add(quad); return dsg; })
                    .map(DatasetFactory::wrap)
                    .toFlowable();
        };

        rdd = enrichRddWithRowNumIfNeeded(rdd, query);
        return JavaRddRxOps.mapPartitions(rdd, mapper);
    }

    /** If the given query mentions a variable ?ROWNUM (upper case) then the input rdd of bindings is
     *  zipped with index */
    public static JavaRDD<Binding> enrichRddWithRowNumIfNeeded(JavaRDD<Binding> rdd, Query query) {
        Set<Var> mentionedVars = query.isConstructType()
                ? QuadPatternUtils.getVarsMentioned(query.getConstructTemplate().getQuads())
                : new HashSet<>();

        Op op = Algebra.compile(query);
        Collection<Var> patternVars = OpVars.mentionedVars(op);
        mentionedVars.addAll(patternVars);
        boolean usesRowNum = mentionedVars.contains(ROWNUM);

        JavaRDD<Binding> result = usesRowNum
                ? rdd.zipWithIndex().map(bi -> BindingFactory.binding(bi._1, ROWNUM, NodeValue.makeInteger(bi._2).asNode()))
                : rdd;

        return result;
    }
}
