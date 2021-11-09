package net.sansa_stack.kryo.jena;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.aksw.jenax.arq.util.binding.BindingUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.binding.BindingFactory;

import java.util.Map;

public class BindingSerializer
    extends Serializer<Binding>
{
    @Override
    public void write(Kryo kryo, Output output, Binding object) {
        Map<Var, Node> map = BindingUtils.toMap(object);
        kryo.writeClassAndObject(output, map);
    }

    @Override
    public Binding read(Kryo kryo, Input input, Class<Binding> type) {
        @SuppressWarnings("unchecked")
        Map<Var, Node> map = (Map<Var, Node>)kryo.readClassAndObject(input);
        BindingBuilder builder = BindingFactory.builder();

        map.forEach(builder::add);

        Binding result = builder.build();
        return result;
    }

}
