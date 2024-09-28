package net.sansa_stack.query.spark.engine;

import org.aksw.rmltk.model.backbone.rml.ILogicalSource;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public interface RmlSourceProcessor {
    JavaRDD<Binding> eval(JavaSparkContext sc, ILogicalSource logicalSource, Binding parentBinding, ExecutionContext execCxt);
}
