package net.sansa_stack.query.spark.engine;

import org.aksw.jena_sparql_api.algebra.utils.OpVar;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.spark.api.java.JavaRDD;

/*
 * An OpExecutor for Spark analoguous to [[org.apache.jena.sparql.engine.main.OpExecutor]]
 */
public interface OpExecutor {
  JavaRDD<Binding> execute(OpProject op, JavaRDD<Binding> rdd);
  JavaRDD<Binding> execute(OpGroup op, JavaRDD<Binding> rdd);
  JavaRDD<Binding> execute(OpOrder op, JavaRDD<Binding> rdd);
  JavaRDD<Binding> execute(OpExtend op, JavaRDD<Binding> rdd);
  JavaRDD<Binding> execute(OpService op, JavaRDD<Binding> rdd);
  JavaRDD<Binding> execute(OpUnion op, JavaRDD<Binding> rdd);
  JavaRDD<Binding> execute(OpDistinct op, JavaRDD<Binding> rdd);
  JavaRDD<Binding> execute(OpReduced op, JavaRDD<Binding> rdd);
  JavaRDD<Binding> execute(OpFilter op, JavaRDD<Binding> rdd);
  JavaRDD<Binding> execute(OpSlice op, JavaRDD<Binding> rdd);
  JavaRDD<Binding> execute(OpJoin op, JavaRDD<Binding> rdd);
  JavaRDD<Binding> execute(OpLateral op, JavaRDD<Binding> rdd);
  JavaRDD<Binding> execute(OpVar op, JavaRDD<Binding> rdd);
  JavaRDD<Binding> execute(OpDisjunction op, JavaRDD<Binding> rdd);

  /* Interface definition is not yet complete */
}
