package net.sansa_stack.kryo.jena;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;

/**
 * Kryo serializer for {@link Triple}.
 *
 * @author Claus Stadler
 */
public class TripleSerializer extends Serializer<Triple> {
    @Override
    public void write(Kryo kryo, Output output, Triple obj) {
        kryo.writeClassAndObject(output, obj.getSubject());
        kryo.writeClassAndObject(output, obj.getPredicate());
        kryo.writeClassAndObject(output, obj.getObject());
    }

    @Override
    public Triple read(Kryo kryo, Input input, Class<Triple> objClass) {
        Node s = (Node) kryo.readClassAndObject(input);
        Node p = (Node) kryo.readClassAndObject(input);
        Node o = (Node) kryo.readClassAndObject(input);
        Triple result = new Triple(s, p, o);
        return result;
    }
}