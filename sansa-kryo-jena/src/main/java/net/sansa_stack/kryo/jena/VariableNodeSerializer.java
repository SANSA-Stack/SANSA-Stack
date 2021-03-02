package net.sansa_stack.kryo.jena;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Node_Variable;

/**
 * Kryo serializer for {@link Node_Variable}.
 *
 * @author Claus Stadler
 */
public class VariableNodeSerializer extends Serializer<Node_Variable> {

    @Override
    public void write(Kryo kryo, Output output, Node_Variable obj) {
        output.writeString(obj.toString());
    }

    @Override
    public Node_Variable read(Kryo kryo, Input input, Class<Node_Variable> objClass) {
        Node_Variable result = (Node_Variable) NodeFactory.createVariable(input.readString());
        return result;
    }

}

