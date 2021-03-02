package net.sansa_stack.kryo.jena;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.util.ExprUtils;

/**
 * Serializer for {@link Expr}.
 *
 * @author Claus Stadler
 */
public class ExprSerializer extends Serializer<Expr> {
    @Override
    public void write(Kryo kryo, Output output, Expr obj) {
        String str = ExprUtils.fmtSPARQL(obj);
        output.writeString(str);
    }

    @Override
    public Expr read(Kryo kryo, Input input, Class<Expr> objClass) {
        String str = input.readString();
        Expr result = ExprUtils.parse(str);
        return result;
    }
}
