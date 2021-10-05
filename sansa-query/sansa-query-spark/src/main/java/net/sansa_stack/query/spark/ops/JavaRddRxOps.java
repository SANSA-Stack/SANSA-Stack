package net.sansa_stack.query.spark.ops;

import io.reactivex.rxjava3.core.Flowable;
import org.aksw.commons.lambda.serializable.SerializableFunction;
import org.aksw.commons.rx.util.RxUtils;
import org.aksw.jena_sparql_api.rx.query_flow.QueryFlowOps;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.spark.api.java.JavaRDD;
import org.reactivestreams.Publisher;

import java.util.Iterator;

public class JavaRddRxOps {

    /** Map operation based on a flowable transformer */
    public static <I, O> JavaRDD<O> map(JavaRDD<I> rdd, SerializableFunction<Flowable<I>, Publisher<O>> transformer) {
        return rdd.mapPartitions(it -> RxUtils.<I, Iterator<I>>fromIteratorSupplier(() -> it)
                .compose(transformer::apply).blockingIterable().iterator());
    }

    public static JavaRDD<Quad> tarql(JavaRDD<Binding> rdd, Query query) {
        return map(rdd, QueryFlowOps.createMapperQuads(query)::apply);
    }
}
