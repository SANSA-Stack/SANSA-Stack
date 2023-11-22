package net.sansa_stack.spark.io.common;

import net.sansa_stack.spark.io.common.HadoopFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;

public class HadoopOutputFormat
    extends HadoopFormat<OutputFormat>
{
    protected HadoopOutputFormat(Class<?> keyClass, Class<?> valueClass, Class<? extends OutputFormat> formatClass) {
        super(keyClass, valueClass, formatClass);
    }

    public static HadoopOutputFormat of(Class<?> keyClass, Class<?> valueClass, Class<? extends OutputFormat> formatClass) {
        return new HadoopOutputFormat(keyClass, valueClass, formatClass);
    }
}
