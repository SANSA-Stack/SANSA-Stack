package net.sansa_stack.query.spark.ops;

import io.reactivex.rxjava3.core.Flowable;
import org.aksw.commons.lambda.serializable.SerializableFunction;
import org.aksw.commons.rx.util.RxUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;

import java.util.Iterator;

public class JavaRddRxOps {

    /** Map operation based on a flowable transformer */
    public static <I, O> JavaRDD<O> map(JavaRDD<I> rdd, SerializableFunction<Flowable<I>, Flowable<O>> transformer) {
        return rdd.mapPartitions(it -> RxUtils.<I, Iterator<I>>fromIteratorSupplier(() -> it)
                .compose(transformer::apply).blockingIterable().iterator());
    }
}
