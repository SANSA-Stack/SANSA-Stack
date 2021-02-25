package net.sansa_stack.kryo.jena;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.Expr;

import java.util.List;
import java.util.Map;

/**
 * Kryo serializer for {@link VarExprList}.
 *
 * @author Claus Stadler
 */
public class VarExprListSerializer extends Serializer<VarExprList> {
    @Override
    public void write(Kryo kryo, Output output, VarExprList obj) {
        kryo.writeClassAndObject(output, obj.getVars());
        kryo.writeClassAndObject(output, obj.getExprs());
    }

    @Override
    public VarExprList read(Kryo kryo, Input input, Class<VarExprList> objClass) {
        @SuppressWarnings("unchecked")
        List<Var> vars = (List<Var>) kryo.readClassAndObject(input);
        @SuppressWarnings("unchecked")
        Map<Var, Expr> map = (Map<Var, Expr>) kryo.readClassAndObject(input);

        VarExprList result = new VarExprList();
        vars.forEach(v -> {
            Expr e = map.get(v);
            if (e == null) {
                result.add(v);
            } else {
                result.add(v, e);
            }
        });

        return result;
    }
}
