package net.sansa_stack.hadoop.generic;

import org.aksw.jena_sparql_api.mapper.Acc;

public interface Accumulating<T, G, A, U> {
    G classify(T item);
    A createAccumulator(G groupKey);
    void accumulate(A accumulator, T item);
    U accumulatedValue(A accumulator);


    /**
     * Identity accumulator - turns each item into a group that contains only the item and whose value is the item
     *
     * @param <T>
     * @return
     */
    static <T> Accumulating identity() {
        return new Accumulating<T, T, T, T>() {
            @Override
            public T classify(T item) {
                return item;
            }

            @Override
            public T createAccumulator(T groupKey) {
                return groupKey;
            }

            @Override
            public void accumulate(T accumulator, T item) {
                /* nothing to do */
            }

            @Override
            public T accumulatedValue(T accumulator) {
                return accumulator;
            }
        };
    }
}
