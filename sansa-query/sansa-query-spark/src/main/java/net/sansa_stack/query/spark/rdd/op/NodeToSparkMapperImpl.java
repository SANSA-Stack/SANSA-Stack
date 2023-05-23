package net.sansa_stack.query.spark.rdd.op;

import org.aksw.jena_sparql_api.rdf.collections.NodeMapper;
import org.apache.spark.sql.types.DataType;

import java.io.Serializable;
import java.util.Objects;

public class NodeToSparkMapperImpl
    implements NodeToSparkMapper, Serializable
{
    protected DataType sparkDatatype;
    protected NodeMapper<?> nodeMapper;

    public NodeToSparkMapperImpl(DataType sparkDatatype, NodeMapper<?> nodeMapper) {
        this.sparkDatatype = sparkDatatype;
        this.nodeMapper = nodeMapper;
    }

    @Override
    public DataType getSparkDatatype() {
        return sparkDatatype;
    }

    @Override
    public NodeMapper<?> getNodeMapper() {
        return nodeMapper;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeToSparkMapperImpl that = (NodeToSparkMapperImpl) o;
        return Objects.equals(sparkDatatype, that.sparkDatatype) && Objects.equals(nodeMapper, that.nodeMapper);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sparkDatatype, nodeMapper);
    }

    @Override
    public String toString() {
        return "NodeToSparkMapperImpl{" +
                "sparkDatatype=" + sparkDatatype +
                ", nodeMapper=" + nodeMapper +
                '}';
    }
}
