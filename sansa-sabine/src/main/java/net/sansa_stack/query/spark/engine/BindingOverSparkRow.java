package net.sansa_stack.query.spark.engine;

import org.aksw.jena_sparql_api.rdf.collections.NodeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBase;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class BindingOverSparkRow
    extends BindingBase
{
    protected Row row;
    protected NodeMapper<?> nodeMapper;

    public BindingOverSparkRow(Binding _parent, Row row, NodeMapper<?> nodeMapper) {
        super(_parent);
        this.row = row;
        this.nodeMapper = nodeMapper;
    }

    @Override
    protected Iterator<Var> vars1() {
        StructType schema = row.schema();
        return Arrays.asList(schema.fieldNames()).stream().map(Var::alloc).iterator();
    }

    @Override
    protected int size1() {
        StructType schema = row.schema();
        return schema.size();
    }

    @Override
    protected boolean isEmpty1() {
        StructType schema = row.schema();
        return schema.isEmpty();
    }

    @Override
    protected boolean contains1(Var var) {
        return false;
    }

    @Override
    protected Node get1(Var var) {
        Node result;
        try {
            int index = row.fieldIndex(var.getVarName());
            // TODO We could avoid the boxing with a row-specific node mapper
            Object obj = row.get(index);
            result = obj == null ? null : nodeMapper.toNodeFromObject(obj);
        } catch (IllegalArgumentException e) {
            result = null;
        }
        return result;
    }
}
