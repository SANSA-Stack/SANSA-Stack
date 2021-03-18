package net.sansa_stack.kryo.jena;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_ANY;

/**
 * Kryo serializer for {@link Node_ANY}.
 * Node_ANY is assumed to be a singleton with its instance being {@link Node#ANY};
 * hence no bytes are written.
 *
 * @author Claus Stadler
 */
public class ANYNodeSerializer extends Serializer<Node_ANY> {
    @Override
    public void write(Kryo kryo, Output output, Node_ANY obj) {
    }

    @Override
    public Node_ANY read(Kryo kryo, Input input, Class<Node_ANY> objClass) {
        return (Node_ANY)Node.ANY;
    }
}
