package net.sansa_stack.kryo.jenax;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.jena.query.Dataset;

import java.util.Set;

/**
 * Serializer for NodesInDataset.
 *
 * Currently this implementation only supports NodesInDatasetImpl
 *
 * @param <T>
 */
/*
public class NodesInDatasetSerializer<T extends NodesInDataset> extends Serializer<T> {

    public NodesInDatasetSerializer() {
        super();
    }

    public void write(Kryo kryo, Output output, T rdfNodeInDataset) {
        kryo.writeClassAndObject(output, rdfNodeInDataset.getGraphNameAndNodes());
        kryo.writeClassAndObject(output, rdfNodeInDataset.getDataset());
    }

    public T read(Kryo kryo, Input input, Class<T> clazz) {
        Set<GraphNameAndNode> nodes = (Set<GraphNameAndNode>)kryo.readClassAndObject(input);
        Dataset dataset = (Dataset)kryo.readClassAndObject(input);
        NodesInDataset result = new NodesInDatasetImpl(dataset, nodes);

        return (T)result;
    }
}
*/