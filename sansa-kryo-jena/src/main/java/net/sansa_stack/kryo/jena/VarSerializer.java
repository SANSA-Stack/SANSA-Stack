package net.sansa_stack.kryo.jena;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;

/**
 * Kryo serializer for {@link Var}.
 *
 * @author Claus Stadler
 */
public class VarSerializer extends Serializer<Var> {
    @Override
    public void write(Kryo kryo, Output output, Var obj) {
        output.writeString(obj.getName());
    }

    @Override
    public Var read(Kryo kryo, Input input, Class<Var> objClass) {
        Var result = Var.alloc(input.readString());
        return result;
    }
}
