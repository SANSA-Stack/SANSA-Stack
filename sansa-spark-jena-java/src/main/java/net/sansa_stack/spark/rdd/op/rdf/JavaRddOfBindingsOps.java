package net.sansa_stack.spark.rdd.op.rdf;

import com.google.common.base.Preconditions;
import net.sansa_stack.spark.rdd.op.rx.JavaRddRxOps;
import net.sansa_stack.spark.util.JavaSparkContextUtils;
import org.aksw.commons.rx.function.RxFunction;
import org.aksw.jena_sparql_api.rx.query_flow.QueryFlowOps;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.syntax.Template;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sparql.util.NodeFactoryExtra;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class JavaRddOfBindingsOps {

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

        return JavaRddRxOps.mapPartitions(rdd, mapper);
    }
}
