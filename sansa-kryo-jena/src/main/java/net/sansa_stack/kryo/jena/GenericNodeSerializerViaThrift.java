package net.sansa_stack.kryo.jena;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.jena.graph.Node;

/**
 * Node serializer using Jena's thrift implementation.
 *
 * @author Claus Stadler
 */
public class GenericNodeSerializerViaThrift extends Serializer<Node> {

    protected boolean allowValues;

    public GenericNodeSerializerViaThrift() {
        this(false);
    }

    /**
     * @param allowValues Whether to encode Nodes as values where possible. May break term equivalence e.g. for doubles.
     */
    public GenericNodeSerializerViaThrift(boolean allowValues) {
        super();
        this.allowValues = allowValues;
    }

    @Override
    public void write(Kryo kryo, Output output, Node node) {
        byte[] buffer = ThriftUtils.writeNode(node, allowValues);
        ByteArrayUtils.write(output, buffer);
    }

    @Override
    public Node read(Kryo kryo, Input input, Class<Node> nodeClass) {
        byte[] buffer = ByteArrayUtils.read(input);
        Node result = ThriftUtils.readNode(buffer);
        return result;
    }
}
