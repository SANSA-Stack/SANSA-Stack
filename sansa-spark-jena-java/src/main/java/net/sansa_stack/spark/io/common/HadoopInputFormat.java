package net.sansa_stack.spark.io.common;

import org.apache.hadoop.mapreduce.InputFormat;

public class HadoopInputFormat
    extends HadoopFormat<InputFormat>
{
    protected HadoopInputFormat(Class<?> keyClass, Class<?> valueClass, Class<? extends InputFormat> formatClass) {
        super(keyClass, valueClass, formatClass);
    }

    public static HadoopFormat of(Class<?> keyClass, Class<?> valueClass, Class<? extends InputFormat> formatClass) {
        return new HadoopFormat(keyClass, valueClass, formatClass);
    }
}
