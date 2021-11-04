package net.sansa_stack.kryo.jenax;

import org.aksw.jenax.sparql.relation.dataset.GraphNameAndNode;
import org.apache.jena.graph.Node;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class GraphNameAndNodeSerializer extends Serializer<GraphNameAndNode> {

    public GraphNameAndNodeSerializer() {
        super();
    }

    public void write(Kryo kryo, Output output, GraphNameAndNode graphNameAndNode) {
        kryo.writeObject(output, graphNameAndNode.getGraphName());
        kryo.writeClassAndObject(output, graphNameAndNode.getNode());
    }

    public GraphNameAndNode read(Kryo kryo, Input input, Class<GraphNameAndNode> clazz) {
        String graphName = kryo.readObject(input, String.class);
        Node node = (Node)kryo.readClassAndObject(input);

        return new GraphNameAndNode(graphName, node);
    }
}
