package net.sansa_stack.spark.rdd.op.rdf;

import com.google.common.collect.Streams;
import org.aksw.commons.util.stream.StreamFunction;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Collection;
import java.util.Collections;
import java.util.function.BiConsumer;
import java.util.stream.Collector;

public class JavaRddOps {

    public static <T> JavaRDD<T> unionIfNeeded(JavaSparkContext jsc, Collection<JavaRDD<T>> rdds) {
        int n = rdds.size();
        JavaRDD<T> result = n == 0
                ? jsc.emptyRDD()
                : n == 1
                    ? rdds.iterator().next()
                    : jsc.union(rdds.toArray(new JavaRDD[0]));
        return result;
    }

    /**
     * Convenience helper to group values by keys, optionally sort them and reduce the values.
     *
     * @return A new rdd with grouped and/or sorted keys and merged values according to specification
     */
    public static <K, V> JavaPairRDD<K, V> groupKeysAndReduceValues(
            JavaPairRDD<K, V> rdd,
            boolean distinct,
            boolean sortGraphsByIri,
            int numPartitions,
            Function2<V, V, V> reducer) {
        JavaPairRDD<K, V> resultRdd = rdd;

        if (distinct) {
            resultRdd = resultRdd.reduceByKey(reducer);
        }

        if (numPartitions > 0) {
            if (sortGraphsByIri) {
                resultRdd = resultRdd.repartitionAndSortWithinPartitions(new HashPartitioner(numPartitions));
            } else {
                resultRdd = resultRdd.repartition(numPartitions);
            }
        }

        if (sortGraphsByIri) {
            resultRdd = resultRdd.sortByKey();
        }

        return resultRdd;
    }

    /**
     * Aggregate a JavaRDD using a serializable Collector.
     * Such collectors can be created e.g. using {@link org.aksw.commons.collector.core.AggBuilder}.
     */
    public static <T, A, R> R aggregateUsingJavaCollector(JavaRDD<? extends T> rdd, Collector<? super T, A, R> collector) {
        A unfinishedResult = rdd
            .mapPartitions(it -> {
                A result = collector.supplier().get();
                BiConsumer<A, ? super T> accumulator = collector.accumulator();
                while (it.hasNext()) {
                    T item = it.next();
                    accumulator.accept(result, item);
                }
                return Collections.singleton(result).iterator();
            })
            .reduce(collector.combiner()::apply);

        R finishedResult = collector.finisher().apply(unfinishedResult);
        return finishedResult;
    }

    /** Map operation based on a flowable transformer */
    public static <I, O> JavaRDD<O> mapPartitions(JavaRDD<I> rdd, StreamFunction<I, O> fn) {
        return rdd.mapPartitions(it -> fn.apply(Streams.stream(it)).iterator());
    }

    public static <K, V, O> JavaRDD<O> mapPartitions(JavaPairRDD<K, V> rdd, StreamFunction<Tuple2<K, V>, O> fn) {
        return rdd.mapPartitions(it -> fn.apply(Streams.stream(it)).iterator());
    }
}
