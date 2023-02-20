package net.sansa_stack.spark.io.csv.input;

import javax.naming.Binding;
import java.util.function.Function;

public interface CsvRowMapperFactory
    extends Function<String[][], Function<String[], Binding>>
{
}
