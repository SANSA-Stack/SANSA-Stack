package net.sansa_stack.spark.rdd.op.rdf;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import java.util.Collections;
import java.util.function.BiConsumer;
import java.util.stream.Collector;

public class JavaRddOps {

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
}
