package net.sansa_stack.spark.rdd.op.rx;

import io.reactivex.rxjava3.core.Flowable;
import net.sansa_stack.spark.rdd.function.JavaRddFunction;
import org.aksw.commons.rx.function.RxFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class JavaRddRxOps {


    /**
     * Wrap a {@link RxFunction} as a {@link JavaRddFunction}
     */
    public static <I, O> JavaRddFunction<I, O> asJavaRddFunction(RxFunction<I, O> transformer) {
        return rdd -> mapPartitions(rdd, transformer);
    }

    /** Map operation based on a flowable transformer */
    public static <I, O> JavaRDD<O> mapPartitions(JavaRDD<I> rdd, RxFunction<I, O> transformer) {
        return rdd.mapPartitions(it -> Flowable.fromIterable(() -> it)
                .compose(transformer).blockingIterable().iterator());
    }

    public static <K, V, O> JavaRDD<O> mapPartitions(JavaPairRDD<K, V> rdd, RxFunction<Tuple2<K, V>, O> transformer) {
        return rdd.mapPartitions(it -> Flowable.fromIterable(() -> it)
                .compose(transformer).blockingIterable().iterator());
    }

}
