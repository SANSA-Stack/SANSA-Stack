package net.sansa_stack.kryo.jena;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.jena.graph.Node;

/**
 * Kryo serializer for array of {@link Node}.
 *
 * @author Claus Stadler
 */
public class NodeArraySerializer extends Serializer<Node[]> {
    @Override
    public void write(Kryo kryo, Output output, Node[] obj) {
        KryoArrayUtils.write(kryo, output, obj);
    }

    @Override
    public Node[] read(Kryo kryo, Input input, Class<Node[]> objClass) {
        return KryoArrayUtils.read(kryo, input, Node.class);
    }
}
