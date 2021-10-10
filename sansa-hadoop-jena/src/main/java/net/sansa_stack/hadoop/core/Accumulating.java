package net.sansa_stack.hadoop.core;

/**
 *
 * @param <T> The item type
 * @param <G> The group key type - items are mapped to group keys
 * @param <A> The accumulator type
 * @param <U> The 'finalizer' - maps an accumulator to a final value
 */
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
