package net.sansa_stack.kryo.jena;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;

/**
 * Serializer for {@link Quad}.
 *
 * @author Claus Stadler
 */
public class QuadSerializer extends Serializer<Quad> {
    @Override
    public void write(Kryo kryo, Output output, Quad obj) {
        kryo.writeClassAndObject(output, obj.getGraph());
        kryo.writeClassAndObject(output, obj.getSubject());
        kryo.writeClassAndObject(output, obj.getPredicate());
        kryo.writeClassAndObject(output, obj.getObject());
    }

    @Override
    public Quad read(Kryo kryo, Input input, Class<Quad> objClass) {
        Node g = (Node) kryo.readClassAndObject(input);
        Node s = (Node) kryo.readClassAndObject(input);
        Node p = (Node) kryo.readClassAndObject(input);
        Node o = (Node) kryo.readClassAndObject(input);
        Quad result = new Quad(g, s, p, o);
        return result;
    }
}
