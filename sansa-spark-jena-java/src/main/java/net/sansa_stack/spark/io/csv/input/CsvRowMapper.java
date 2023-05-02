package net.sansa_stack.spark.io.csv.input;

import org.apache.jena.sparql.engine.binding.Binding;

import java.util.function.Function;

public interface CsvRowMapper
    extends Function<String[], Binding>
{
}
