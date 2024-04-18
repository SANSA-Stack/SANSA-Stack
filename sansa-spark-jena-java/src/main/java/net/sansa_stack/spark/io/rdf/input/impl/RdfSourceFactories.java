package net.sansa_stack.spark.io.rdf.input.impl;

import net.sansa_stack.spark.io.rdf.input.api.RdfSourceFactory;
import org.apache.spark.sql.SparkSession;

public class RdfSourceFactories {
    public static RdfSourceFactory of(SparkSession sparkSession) {
        return new RdfSourceFactoryImpl(sparkSession);
    }
}
