package net.sansa_stack.query.spark.rdd.op;

import org.aksw.jena_sparql_api.rdf.collections.NodeMapper;
import org.apache.spark.sql.types.DataType;

public interface NodeToSparkMapper {
    DataType getSparkDatatype();
    NodeMapper<?> getNodeMapper();
}
