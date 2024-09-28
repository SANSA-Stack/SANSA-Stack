package net.sansa_stack.spark.io.rdf.input.api;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public abstract class RddLoaderBase<K, T>
	implements RddLoader<K, T>
{
    protected Class<T> clazz;
    protected Class<? extends FileInputFormat<K, T>> fileInputFormatClass;

    public RddLoaderBase(Class<T> clazz, Class<? extends FileInputFormat<K, T>> fileInputFormatClass) {
        this.clazz = clazz;
        this.fileInputFormatClass = fileInputFormatClass;
    }

    @Override
    public Class<T> getValueClass() {
        return clazz;
    }

    @Override
    public Class<? extends FileInputFormat<K, T>> getFileInputFormatClass() {
        return fileInputFormatClass;
    }
}
