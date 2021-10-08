package net.sansa_stack.rdf.spark.io.input.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.jena.hadoop.rdf.types.AbstractNodeTupleWritable;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

public interface RddRdfLoader<T> {

    RDD<T> load(SparkContext sparkContext, String path);


    public static <T> JavaRDD<T> createJavaRdd(
            SparkContext sparkContext,
            String path,
            Class<T> clazz,
            Class<? extends FileInputFormat<LongWritable, T>> fileInputFormat) {
        return asJavaRdd(createRddOfDatasetCore(sparkContext, path, clazz, fileInputFormat));
    }

    public static <T> RDD<T> createRdd(
            SparkContext sparkContext,
            String path,
            Class<T> clazz,
            Class<? extends FileInputFormat<LongWritable, T>> fileInputFormat) {
        return createJavaRdd(sparkContext, path, clazz, fileInputFormat).rdd();
    }



//
//    public static <T> RDD<T> createRddElephas(
//            SparkContext sparkContext,
//            String path,
//            Class<T> clazz,
//            Class<? extends FileInputFormat<LongWritable, ? extends AbstractNodeTupleWritable<T>>> fileInputFormat) {
//        return createJavaRdd(sparkContext, path, clazz, fileInputFormat).rdd();
//    }
//
//    public static <T> RDD<Tuple2<LongWritable, T>> createRddOfDatasetCoreElephas(
//            SparkContext sparkContext,
//            String path,
//            Class<T> clazz,
//            Class<? extends FileInputFormat<LongWritable, ? extends AbstractNodeTupleWritable<T>>> fileInputFormat) {
//        Configuration confHadoop = sparkContext.hadoopConfiguration();
//
//        RDD<Tuple2<LongWritable, T>> result = sparkContext
//            .newAPIHadoopFile(
//                path,
//                fileInputFormat,
//                LongWritable.class,
//                clazz,
//                confHadoop);
//
//        return result;
//    }
//



    public static <T> RDD<Tuple2<LongWritable, T>> createRddOfDatasetCore(
            SparkContext sparkContext,
            String path,
            Class<T> clazz,
            Class<? extends FileInputFormat<LongWritable, T>> fileInputFormat) {
        Configuration confHadoop = sparkContext.hadoopConfiguration();

        RDD<Tuple2<LongWritable, T>> result = sparkContext
            .newAPIHadoopFile(
                path,
                fileInputFormat,
                LongWritable.class,
                clazz,
                confHadoop);

        return result;
    }

    /** Tiny helper to get the desired JavaRDD */
    public static <T> JavaRDD<T> asJavaRdd(RDD<Tuple2<LongWritable, T>> rdd) {
        return rdd.toJavaRDD().map(t -> t._2());
    }

//    public static loadAsDataset(SparkSession sparkSession, String pathStr) {
//
//    }
}