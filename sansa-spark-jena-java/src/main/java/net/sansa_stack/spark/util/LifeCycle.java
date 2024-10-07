package net.sansa_stack.spark.util;

public interface LifeCycle<T> {
    T newInstance();
    void closeInstance(T inst);
}
