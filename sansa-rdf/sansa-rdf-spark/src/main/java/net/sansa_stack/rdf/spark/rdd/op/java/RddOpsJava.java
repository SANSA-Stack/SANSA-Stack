package net.sansa_stack.rdf.spark.rdd.op.java;

import org.apache.spark.api.java.JavaRDD;

import java.util.Collections;
import java.util.function.BiConsumer;
import java.util.stream.Collector;

public class RddOpsJava {
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
