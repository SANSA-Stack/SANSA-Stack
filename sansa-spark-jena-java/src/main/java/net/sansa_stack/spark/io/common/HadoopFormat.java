package net.sansa_stack.spark.io.common;

/**
 * This class bundles hadoop format information: keyClass, valueClass and formatClass.
 */
public class HadoopFormat<T> {
    protected Class<?> keyClass;
    protected Class<?> valueClass;
    protected Class<? extends T> formatClass;

    protected HadoopFormat(Class<?> keyClass, Class<?> valueClass, Class<? extends T> formatClass) {
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.formatClass = formatClass;
    }

    public Class<?> getKeyClass() {
        return keyClass;
    }

    public Class<?> getValueClass() {
        return valueClass;
    }

    public Class<? extends T> getFormatClass() {
        return formatClass;
    }
}
